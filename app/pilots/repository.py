from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


# 节点实际占用的场次：running 与 cancel_requested（等待安全停止）都占名额，
# 只有完成/失败/取消确认/超时恢复改变状态后才释放；租约过期但未恢复的任务仍占位。
# lease_owner 按目录编码（小写）归一化后比较。
OCCUPIED_SELECT = """
SELECT
  SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running,
  SUM(CASE WHEN status='cancel_requested' THEN 1 ELSE 0 END) AS stop_pending,
  COUNT(*) AS total
FROM pilot_sessions
WHERE status IN ('running','cancel_requested')
  AND LOWER(COALESCE(lease_owner,''))=?
"""


class PilotRepository:
    """封装运营游览场次运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def site_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM pilot_sites WHERE code=?", (code,)).fetchone()

    def site_occupancy(self, site_code: str) -> dict[str, int]:
        row = self.connection.execute(OCCUPIED_SELECT, (site_code.lower(),)).fetchone()
        return {"running": int(row["running"] or 0), "stop_pending": int(row["stop_pending"] or 0), "total": int(row["total"] or 0)}

    def queued_candidate(self, capabilities: Iterable[str], now: str, *, max_candidates: int = 1) -> list[sqlite3.Row]:
        """返回公共队列中按优先级排序、可被给定能力领取的候选（仅读取，不分配）。"""
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.capability IN ({placeholders})"
            params.extend(capability_list)
        params.append(max(1, int(max_candidates)))
        rows = self.connection.execute(
            "SELECT t.*,tpl.capability AS protocol_capability FROM pilot_sessions t JOIN pilot_protocols tpl ON tpl.id=t.protocol_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT ?",
            params,
        ).fetchall()
        return list(rows)

    def try_claim_session(self, candidate_id: int, site_code: str, lease_until: str, now: str) -> bool:
        """条件更新：仅当场次仍处于排队状态时才占用名额，由调用方保证节点尚有容量。"""
        cursor = self.connection.execute(
            "UPDATE pilot_sessions SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
            (site_code, lease_until, now, now, candidate_id),
        )
        return cursor.rowcount == 1

    def protocol_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM pilot_protocols WHERE code=?", (code,)).fetchone()

    def protocol_by_id(self, protocol_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM pilot_protocols WHERE id=?", (protocol_id,)).fetchone()

    def active_protocols(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM pilot_protocols WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_protocol(self, *, code: str, name: str, capability: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, created_by: str, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO pilot_protocols(code,name,capability,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,1,?,?,?)",
            (code, name, capability, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, created_by, now, now),
        )
        return dict(self.protocol_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM pilot_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO pilot_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM pilot_sessions WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM pilot_sessions WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def session_by_id(self, session_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS protocol_code,tpl.capability AS protocol_capability FROM pilot_sessions t JOIN pilot_protocols tpl ON tpl.id=t.protocol_id WHERE t.id=?", (session_id,)).fetchone()

    def session_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM pilot_sessions WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_session(self, *, protocol_id: int, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO pilot_sessions(protocol_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?)",
            (protocol_id, project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, now, now),
        )
        return dict(self.session_by_id(cursor.lastrowid))

    def observation_versions(self, session_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM pilot_observations WHERE session_id=? ORDER BY version", (session_id,)).fetchall()]

    def interventions(self, session_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM pilot_interventions WHERE session_id=? ORDER BY id", (session_id,)).fetchall()]

    def add_intervention(self, *, session_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO pilot_interventions(session_id,actor,action,reason,before_json,after_json,batch_key,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (session_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, now),
        )

    def list_sessions(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS protocol_code,tpl.capability AS protocol_capability FROM pilot_sessions t JOIN pilot_protocols tpl ON tpl.id=t.protocol_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]


