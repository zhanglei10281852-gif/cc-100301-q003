from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.pilots.repository import PilotRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class PilotOperationsService:
    """管理运营方案、配额、游览场次租约、观察记录版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = PilotRepository(self.connection)

    def list_protocols(self) -> list[dict[str, Any]]:
        return self.repository.active_protocols()

    def create_protocol(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            if repository.protocol_by_code(payload["code"]):
                raise ConflictError("参数方案编码已存在")
            return repository.create_protocol(
                code=payload["code"], name=payload["name"], capability=payload["capability"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return PilotRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            protocol = repository.protocol_by_code(payload["protocol_code"])
            if protocol is None or not protocol["active"]:
                raise NotFoundError("参数方案不存在或已经停用")
            parameters = self._validate_parameters(protocol, payload["parameters"])
            existing = repository.session_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的运营参数")
                return dict(repository.session_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_session(
                protocol_id=protocol["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=protocol["max_attempts"], now=now,
            )

    def list_sessions(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_sessions(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_session(self, session_id: int) -> dict[str, Any]:
        row = self.repository.session_by_id(session_id)
        if row is None:
            raise NotFoundError("运营游览场次不存在")
        observation = dict(row)
        observation["observations"] = self.repository.observation_versions(session_id)
        observation["interventions"] = self.repository.interventions(session_id)
        return observation

    def claim(self, site_code: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any]:
        """节点以真实可用容量为前提领取公共队列中的任务。

        必须同时满足：节点存在且处于 active、节点目录能力与请求能力有交集、运行中与
        等待安全停止（cancel_requested）的任务合计未达到 max_concurrent。
        任何条件不满足时不占用队列任务，返回当前占用与拒绝原因。
        整个判定在 BEGIN IMMEDIATE 事务内完成，配合同步容量检查，并发领取同一空余
        名额时只有一个节点成功；租约过期但未恢复的任务仍占位，由恢复入口回收。
        """
        raw_site_code = site_code.strip()
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            site = repository.site_by_code(raw_site_code.lower())
            if site is None:
                raise NotFoundError("目的地节点不存在")
            occupancy = repository.site_occupancy(raw_site_code)
            capacity = int(site["max_concurrent"])
            result: dict[str, Any] = {
                "site_code": site["code"],
                "site_status": site["status"],
                "capacity": capacity,
                "running_sessions": occupancy["running"],
                "stop_pending_sessions": occupancy["stop_pending"],
                "occupied_sessions": occupancy["total"],
                "available_slots": max(0, capacity - occupancy["total"]),
            }
            if site["status"] != "active":
                result.update(session=None, accepted=False, reason="site_not_serviceable",
                              message="节点当前不在可服务状态（已暂停或关闭）")
                return result
            site_capabilities = set(json.loads(site["capabilities_json"]))
            requested_capabilities = set(capabilities)
            effective_capabilities = site_capabilities & requested_capabilities if requested_capabilities else site_capabilities
            if not effective_capabilities:
                result.update(session=None, accepted=False, reason="capability_mismatch",
                              message="节点能力与领取请求不匹配，公共队列任务保留给其他节点")
                return result
            if occupancy["total"] >= capacity:
                result.update(session=None, accepted=False, reason="capacity_exhausted",
                              message=f"节点容量已用尽（{occupancy['total']}/{capacity}），等待安全停止的任务仍占用名额")
                return result
            # 多取候选以应对候选刚被其他节点抢走的并发场景；条件更新保证同一任务只会被领取一次。
            candidates = repository.queued_candidate(effective_capabilities, now, max_candidates=capacity + 1)
            for candidate in candidates:
                if repository.try_claim_session(int(candidate["id"]), site["code"], lease_until, now):
                    claimed = dict(repository.session_by_id(int(candidate["id"])))
                    result.update(session=claimed, accepted=True, reason="",
                                  running_sessions=occupancy["running"] + 1,
                                  occupied_sessions=occupancy["total"] + 1,
                                  available_slots=max(0, capacity - occupancy["total"] - 1))
                    return result
            result.update(available_slots=max(0, capacity - occupancy["total"]),
                          session=None, accepted=False, reason="no_matching_queued_session",
                          message="公共队列中没有可领取的匹配任务")
            return result

    def heartbeat(self, session_id: int, site_code: str, lease_seconds: int) -> dict[str, Any]:
        site_code = site_code.strip().lower()
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            cursor = connection.execute(
                "UPDATE pilot_sessions SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, session_id, site_code),
            )
            if cursor.rowcount != 1:
                raise ConflictError("游览场次未由当前执行站点持有")
            return dict(PilotRepository(connection).session_by_id(session_id))

    def complete(self, session_id: int, site_code: str, observation: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        site_code = site_code.strip().lower()
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            if session["status"] != "running" or session["lease_owner"] != site_code:
                raise ConflictError("游览场次未由当前执行站点持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM pilot_observations WHERE session_id=?", (session_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO pilot_observations(session_id,version,observation_json,metrics_json,observation_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (session_id, version, json.dumps(observation, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"observation": observation, "metrics": metrics}), site_code, now),
            )
            connection.execute(
                "UPDATE pilot_sessions SET status='succeeded',current_observation_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, session_id),
            )
            return dict(repository.session_by_id(session_id))

    def fail(self, session_id: int, site_code: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        site_code = site_code.strip().lower()
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            if session["status"] != "running" or session["lease_owner"] != site_code:
                raise ConflictError("游览场次未由当前执行站点持有")
            can_retry = retryable and int(session["attempt_count"]) < int(session["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(session["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE pilot_sessions SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, session_id),
            )
            return dict(repository.session_by_id(session_id))

    def cancel(self, session_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        return self._intervene(session_id, actor, reason, "cancel", batch_key, self._cancel_mutation)

    def retry(self, session_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
            if session["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消游览场次可以人工重试")
            chosen = session["priority"] if priority is None else priority
            connection.execute("UPDATE pilot_sessions SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, session["id"]))
        return self._intervene(session_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, session_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
            if session["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的游览场次可以调整优先级")
            connection.execute("UPDATE pilot_sessions SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, session["id"]))
        return self._intervene(session_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "session_ids": payload["session_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for session_id in list(dict.fromkeys(payload["session_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(session_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(session_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(session_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"session_id": session_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"session_id": session_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def acknowledge_stop(self, session_id: int, site_code: str) -> dict[str, Any]:
        """节点确认等待安全停止的场次已经停下，立即释放占用名额。"""
        site_code = site_code.strip().lower()
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            if session["status"] != "cancel_requested":
                raise ConflictError("只有等待安全停止的游览场次可以确认停止")
            if session["lease_owner"] != site_code:
                raise ConflictError("游览场次未由当前执行站点持有")
            before = dict(session)
            connection.execute(
                "UPDATE pilot_sessions SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (now, now, session_id),
            )
            after = dict(repository.session_by_id(session_id))
            repository.add_intervention(session_id=session_id, actor=site_code, action="stop_confirmed", reason="节点确认安全停止并释放容量", before=before, after=after, batch_key="", now=now)
            return after

    def recover_expired(self, actor: str = "recovery-site") -> dict[str, Any]:
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            rows = connection.execute("SELECT * FROM pilot_sessions WHERE status IN ('running','cancel_requested') AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id", (now,)).fetchall()
            for session in rows:
                before = dict(session)
                if session["status"] == "cancel_requested":
                    # 取消请求等待安全停止直到租约超时：超时后按已取消落库并释放容量。
                    status, finished_at, action, reason = "cancelled", now, "cancel_recovery", "取消请求租约超时，按已取消恢复容量"
                    error_code, error_message = session["last_error_code"], session["last_error_message"]
                    cancelled.append(int(session["id"]))
                elif int(session["attempt_count"]) < int(session["max_attempts"]):
                    status, finished_at, action, reason = "queued", None, "lease_recovery", "租约过期自动恢复"
                    error_code, error_message = "lease_expired", "执行站点租约已过期"
                    recovered.append(int(session["id"]))
                else:
                    status, finished_at, action, reason = "failed", now, "lease_recovery", "租约过期且尝试次数用尽"
                    error_code, error_message = "lease_expired", "执行站点租约已过期"
                    exhausted.append(int(session["id"]))
                connection.execute(
                    "UPDATE pilot_sessions SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, error_code, error_message, finished_at, now, session["id"]),
                )
                after = dict(repository.session_by_id(session["id"]))
                repository.add_intervention(session_id=session["id"], actor=actor, action=action, reason=reason, before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM pilot_sessions GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM pilot_sessions WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "protocols": len(self.repository.active_protocols())}

    def _intervene(self, session_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            before = dict(session)
            mutation(connection, session, now)
            after = dict(repository.session_by_id(session_id))
            repository.add_intervention(session_id=session_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    @staticmethod
    def _cancel_mutation(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
        if session["status"] not in {"queued", "running"}:
            raise ConflictError("当前游览场次状态不允许取消")
        status = "cancel_requested" if session["status"] == "running" else "cancelled"
        connection.execute("UPDATE pilot_sessions SET status=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?", (status, None if status == "cancel_requested" else now, now, session["id"]))

    def _check_quota(self, repository: PilotRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队游览场次配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行游览场次配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数方案至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, protocol: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(protocol["parameter_schema_json"])
        values = {**json.loads(protocol["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含方案未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized


