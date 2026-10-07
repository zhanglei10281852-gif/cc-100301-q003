from __future__ import annotations

import threading
from datetime import UTC, datetime

import pytest

from app.catalog.service import CatalogService
from app.core.clock import FrozenClock
from app.database import get_connection
from app.pilots.service import PilotOperationsService


PROTOCOL = {
    "code": "county-shuttle",
    "name": "县域慢游米轨接驳方案",
    "capability": "rail-shuttle",
    "parameter_schema": {"minutes": {"type": "integer", "required": True, "minimum": 1, "maximum": 60}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}

OTHER_PROTOCOL = {
    "code": "night-tour",
    "name": "常态夜游动线方案",
    "capability": "night-tour",
    "parameter_schema": {"scene": {"type": "string", "required": True}},
    "default_parameters": {},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def site_payload(code: str, *, capabilities=None, max_concurrent: int = 1, status: str = "active") -> dict:
    return {
        "code": code,
        "name": f"节点 {code}",
        "site_type": "交通枢纽",
        "region": "测试县域",
        "capabilities": ["rail-shuttle"] if capabilities is None else capabilities,
        "max_concurrent": max_concurrent,
        **({"status": status} if status != "active" else {}),
    }


def submit_payload(key: str, *, protocol: str = "county-shuttle", user: str = "ops-1") -> dict:
    parameters = {"minutes": 12} if protocol == "county-shuttle" else {"scene": "old-street"}
    return {
        "protocol_code": protocol,
        "project_code": "county-slow-tour",
        "requested_by": user,
        "parameters": parameters,
        "priority": 50,
        "idempotency_key": key,
    }


@pytest.fixture()
def world(client):
    """通过 API 初始化方案、节点并返回直接操作服务层的句柄。"""
    client.post("/api/pilots/protocols?actor=admin", json=PROTOCOL)
    client.post("/api/pilots/protocols?actor=admin", json=OTHER_PROTOCOL)
    client.post("/api/catalog/sites", json=site_payload("rail-1", max_concurrent=1))
    client.post("/api/catalog/sites", json=site_payload("rail-2", max_concurrent=1))
    client.post("/api/catalog/sites", json=site_payload("rail-wide", max_concurrent=3))
    client.post("/api/catalog/sites", json=site_payload("rail-off", max_concurrent=1))
    suspended = client.patch("/api/catalog/sites/rail-off", json={"status": "suspended"})
    assert suspended.status_code == 200, suspended.text
    client.post("/api/catalog/sites", json=site_payload("night-only", capabilities=["night-tour"], max_concurrent=1))
    return client


def occupied_now(site_code: str) -> int:
    row = get_connection().execute(
        "SELECT COUNT(*) FROM pilot_sessions WHERE status IN ('running','cancel_requested') "
        "AND LOWER(COALESCE(lease_owner,''))=?",
        (site_code.lower(),),
    ).fetchone()
    return int(row[0])


def test_single_capacity_node_holds_at_most_one_task_and_failed_claim_keeps_queue(world):
    for index in range(2):
        world.post("/api/pilots/sessions", json=submit_payload(f"single-{index:06d}"))

    first = world.post("/api/pilots/sessions/claim", json={"site_code": "RAIL-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert first.status_code == 200
    body = first.json()
    assert body["accepted"] is True
    assert body["session"] is not None
    assert body["occupied_sessions"] == 1
    assert body["running_sessions"] == 1
    assert body["available_slots"] == 0

    second = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert second.status_code == 200
    rejected = second.json()
    assert rejected["accepted"] is False
    assert rejected["reason"] == "capacity_exhausted"
    assert rejected["session"] is None
    assert rejected["running_sessions"] == 1
    assert rejected["stop_pending_sessions"] == 0
    assert rejected["occupied_sessions"] == 1
    assert rejected["available_slots"] == 0
    assert rejected["capacity"] == 1

    # 被拒领取没有丢项：任务仍在公共队列，可由其他节点领走。
    queued = world.get("/api/pilots/sessions?status=queued").json()["items"]
    assert len(queued) == 1
    other = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-2", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert other.json()["accepted"] is True
    assert other.json()["session"]["id"] == queued[0]["id"]
    assert occupied_now("rail-1") == 1

    # 用大写编码回执与领取编码归一化一致：完成后名额释放。
    done = world.post(
        f"/api/pilots/sessions/{body['session']['id']}/complete",
        json={"site_code": "RAIL-1", "observation": {"ok": True}, "metrics": {}},
    )
    assert done.status_code == 200 and done.json()["status"] == "succeeded"
    assert occupied_now("rail-1") == 0


def test_unknown_node_is_404_and_suspended_node_is_not_serviceable(world):
    world.post("/api/pilots/sessions", json=submit_payload("suspend-000001"))

    missing = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-404", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert missing.status_code == 404

    suspended = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-off", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert suspended.status_code == 200
    body = suspended.json()
    assert body["accepted"] is False
    assert body["reason"] == "site_not_serviceable"
    assert body["site_status"] == "suspended"
    assert body["session"] is None
    # 任务仍保留在公共队列。
    assert len(world.get("/api/pilots/sessions?status=queued").json()["items"]) == 1


def test_capability_mismatch_leaves_task_for_other_nodes(world):
    world.post("/api/pilots/sessions", json=submit_payload("cap-rail-00001"))
    world.post("/api/pilots/sessions", json=submit_payload("cap-night-0001", protocol="night-tour"))

    mismatch = world.post("/api/pilots/sessions/claim", json={"site_code": "night-only", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert mismatch.json()["accepted"] is False
    assert mismatch.json()["reason"] == "capability_mismatch"

    # 请求能力与节点目录能力取交集：目录里只有 night-tour 时领不到接驳任务。
    mismatch_two = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["night-tour"], "lease_seconds": 60})
    assert mismatch_two.json()["reason"] == "capability_mismatch"

    night = world.post("/api/pilots/sessions/claim", json={"site_code": "night-only", "capabilities": ["night-tour"], "lease_seconds": 60})
    assert night.json()["accepted"] is True
    assert night.json()["session"]["protocol_code"] == "night-tour"

    rail = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert rail.json()["accepted"] is True
    assert rail.json()["session"]["protocol_code"] == "county-shuttle"
    assert world.get("/api/pilots/sessions?status=queued").json()["items"] == []


def test_completion_failure_and_cancel_stop_release_capacity(world):
    ids = []
    for index in range(3):
        ids.append(world.post("/api/pilots/sessions", json=submit_payload(f"release-{index:06d}")).json()["id"])

    first = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    second = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    third = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    assert first["accepted"] and second["accepted"] and third["accepted"]
    full = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    assert full["reason"] == "capacity_exhausted" and full["occupied_sessions"] == 3

    # 完成释放一个名额。
    done = world.post(f"/api/pilots/sessions/{ids[0]}/complete", json={"site_code": "rail-wide", "observation": {"ok": True}, "metrics": {}})
    assert done.status_code == 200

    world.post("/api/pilots/sessions", json=submit_payload("release-new-001"))
    after_complete = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    assert after_complete["accepted"]
    assert after_complete["running_sessions"] == 3

    # 可重试失败：任务按退避时间回到队列，立即释放名额。
    failed = world.post(f"/api/pilots/sessions/{ids[1]}/fail", json={"site_code": "rail-wide", "error_code": "road-closed", "message": "前方道路临时管制", "retryable": True})
    assert failed.status_code == 200 and failed.json()["status"] == "queued"
    # 退避窗口内任务不可领取，但名额已空出。
    waiting = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    assert waiting["accepted"] is False
    assert waiting["reason"] == "no_matching_queued_session"
    assert waiting["occupied_sessions"] == 2
    world.post("/api/pilots/sessions", json=submit_payload("release-new-002"))
    retry = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    assert retry["accepted"] and retry["session"]["id"] != ids[1]

    # 取消运行中任务：进入等待安全停止，仍占名额；节点确认停止后释放。
    cancel = world.post(f"/api/pilots/sessions/{ids[2]}/cancel", json={"actor": "manager", "reason": "当日接驳提前收车"})
    assert cancel.json()["status"] == "cancel_requested"
    blocked = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    assert blocked["reason"] == "capacity_exhausted"
    assert blocked["stop_pending_sessions"] == 1
    stop = world.post(f"/api/pilots/sessions/{ids[2]}/acknowledge-stop", json={"site_code": "rail-wide"})
    assert stop.status_code == 200 and stop.json()["status"] == "cancelled"
    world.post("/api/pilots/sessions", json=submit_payload("release-new-003"))
    freed = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-wide", "capabilities": ["rail-shuttle"], "lease_seconds": 60}).json()
    assert freed["accepted"]
    details = world.get(f"/api/pilots/session-details/{ids[2]}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel", "stop_confirmed"]


def test_expired_lease_recovery_releases_capacity(client):
    clock = FrozenClock(datetime(2026, 10, 6, 2, 0, tzinfo=UTC))
    catalog = CatalogService(get_connection(), clock)
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "admin")
    catalog.create_site(site_payload("frozen-rail", max_concurrent=1))

    first = service.submit(submit_payload("expiry-000001"))
    claimed = service.claim("frozen-rail", ["rail-shuttle"], 10)
    assert claimed["accepted"] and claimed["session"]["id"] == first["id"]

    # 租约虽过期但未恢复前，僵尸任务仍占名额：不会被新领取顶替。
    clock.advance(seconds=11)
    blocked = service.claim("frozen-rail", ["rail-shuttle"], 10)
    assert blocked["reason"] == "capacity_exhausted" and blocked["occupied_sessions"] == 1

    recovered = service.recover_expired()
    assert recovered["recovered"] == [first["id"]]
    details = service.get_session(first["id"])
    assert details["status"] == "queued"

    # 恢复后重新领取（第二次尝试）。
    current = service.claim("frozen-rail", ["rail-shuttle"], 10)
    assert current["accepted"] and current["session"]["id"] == first["id"]

    # 等待安全停止的任务在恢复前继续占位，租约超时恢复为已取消后释放名额。
    service.cancel(first["id"], "manager", "接驳取消")
    assert service.claim("frozen-rail", ["rail-shuttle"], 10)["reason"] == "capacity_exhausted"
    clock.advance(seconds=11)
    recovered_cancel = service.recover_expired()
    assert recovered_cancel["cancelled"] == [first["id"]]
    assert service.get_session(first["id"])["status"] == "cancelled"
    empty = service.claim("frozen-rail", ["rail-shuttle"], 10)
    assert empty["reason"] == "no_matching_queued_session"
    assert empty["occupied_sessions"] == 0


def test_manual_capacity_expansion_allows_more_claims(world):
    for index in range(3):
        world.post("/api/pilots/sessions", json=submit_payload(f"expand-{index:06d}"))

    first = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert first.json()["accepted"]
    blocked = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert blocked.json()["reason"] == "capacity_exhausted"

    patched = world.patch("/api/catalog/sites/rail-1", json={"max_concurrent": 3})
    assert patched.status_code == 200 and patched.json()["max_concurrent"] == 3

    second = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    third = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert second.json()["accepted"] and third.json()["accepted"]
    full = world.post("/api/pilots/sessions/claim", json={"site_code": "rail-1", "capabilities": ["rail-shuttle"], "lease_seconds": 60})
    assert full.json()["reason"] == "capacity_exhausted" and full.json()["occupied_sessions"] == 3


def test_concurrent_claims_single_free_slot_only_one_wins(world):
    for index in range(1):
        world.post("/api/pilots/sessions", json=submit_payload("race-one-00001"))

    barrier = threading.Barrier(8)
    results: list[dict] = []
    results_lock = threading.Lock()

    def worker() -> None:
        service = PilotOperationsService(get_connection())
        barrier.wait()
        outcome = service.claim("rail-1", ["rail-shuttle"], 60)
        with results_lock:
            results.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    accepted = [item for item in results if item["accepted"]]
    rejected = [item for item in results if not item["accepted"]]
    assert len(accepted) == 1
    assert {item["reason"] for item in rejected} <= {"capacity_exhausted", "no_matching_queued_session"}
    assert occupied_now("rail-1") == 1
    # 唯一的排队任务要么被领走，要么没有任何丢项（此处必然被领走）。
    assert world.get("/api/pilots/sessions?status=queued").json()["items"] == []


def test_continuous_competition_and_state_changes_never_overcommits(world):
    task_count = 24
    for index in range(task_count):
        world.post("/api/pilots/sessions", json=submit_payload(f"stress-{index:06d}"))

    stop = threading.Event()
    violations: list[str] = []
    violation_lock = threading.Lock()

    def monitor() -> None:
        connection = get_connection()
        while not stop.is_set():
            for code in ("rail-1", "rail-2"):
                capacity = int(connection.execute("SELECT max_concurrent FROM pilot_sites WHERE code=?", (code,)).fetchone()[0])
                used = occupied_now(code)
                if used > capacity:
                    with violation_lock:
                        violations.append(f"{code} holds {used} > {capacity}")

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()

    # rail-1 初始容量 1，运营经理在运行中人工扩容到 2；期间暂停再恢复 rail-2。
    def operator() -> None:
        service = CatalogService(get_connection())
        threading.Event().wait(0.05)
        service.update_site("rail-1", {"max_concurrent": 2})
        threading.Event().wait(0.05)
        service.update_site("rail-2", {"status": "suspended"})
        threading.Event().wait(0.05)
        service.update_site("rail-2", {"status": "active"})

    operator_thread = threading.Thread(target=operator)
    operator_thread.start()

    def worker(code: str) -> None:
        service = PilotOperationsService(get_connection())
        empty_streak = 0
        while empty_streak < 5:
            outcome = service.claim(code, ["rail-shuttle"], 300)
            if outcome["accepted"]:
                empty_streak = 0
                session_id = outcome["session"]["id"]
                service.complete(session_id, code, {"delivered": True}, {})
            elif outcome["reason"] == "no_matching_queued_session":
                empty_streak += 1
                threading.Event().wait(0.005)
            else:
                threading.Event().wait(0.002)

    threads = [threading.Thread(target=worker, args=("rail-1",)) for _ in range(3)]
    threads += [threading.Thread(target=worker, args=("rail-2",)) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    operator_thread.join()
    stop.set()
    monitor_thread.join()

    assert violations == []
    rows = world.get("/api/pilots/sessions?limit=100").json()["items"]
    assert len(rows) == task_count
    assert {row["status"] for row in rows} == {"succeeded"}
    assert occupied_now("rail-1") == 0
    assert occupied_now("rail-2") == 0
