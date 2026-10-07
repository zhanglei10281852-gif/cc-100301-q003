from __future__ import annotations

import threading
from datetime import UTC, datetime

from app.pilots.service import PilotOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, transaction


PROTOCOL = {
    "code": "chapter-transfer",
    "name": "多城章节线路换乘节奏行程方案",
    "capability": "chapter-transfer",
    "parameter_schema": {
        "minutes": {"type": "integer", "required": True, "minimum": 1, "maximum": 30},
        "assist_level": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "scene": {"type": "string", "required": True, "choices": ["rail", "coach"]},
    },
    "default_parameters": {"assist_level": 0.4},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "pilot-operator-1", priority: int = 50) -> dict:
    return {
        "protocol_code": "chapter-transfer",
        "project_code": "seven-city-story-route",
        "requested_by": user,
        "parameters": {"minutes": 8, "scene": "rail"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_protocol(client) -> None:
    response = client.post("/api/pilots/protocols?actor=administrator", json=PROTOCOL)
    assert response.status_code == 201, response.text


def create_site(client, code: str, capabilities: list[str], max_concurrent: int = 1) -> dict:
    response = client.post("/api/catalog/sites", json={
        "code": code,
        "name": f"节点 {code}",
        "site_type": "交通枢纽",
        "region": "测试县域",
        "capabilities": capabilities,
        "max_concurrent": max_concurrent,
    })
    assert response.status_code == 201, response.text
    return response.json()


def claim(client, site_code: str, capabilities: list[str] | None = None, lease_seconds: int = 60) -> dict:
    payload = {"site_code": site_code, "capabilities": capabilities if capabilities is not None else ["chapter-transfer"], "lease_seconds": lease_seconds}
    response = client.post("/api/pilots/sessions/claim", json=payload)
    assert response.status_code == 200, response.text
    return response.json()


def state_counts(client) -> dict[str, int]:
    return client.get("/api/pilots/summary").json()["states"]


def test_protocol_submission_idempotency_and_parameter_validation(client):
    create_protocol(client)
    first = client.post("/api/pilots/sessions", json=submit_payload("request-000001"))
    second = client.post("/api/pilots/sessions", json=submit_payload("request-000001"))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    invalid = submit_payload("request-000002")
    invalid["parameters"]["minutes"] = 50
    rejected = client.post("/api/pilots/sessions", json=invalid)
    assert rejected.status_code == 422


def test_priority_capability_claim_and_observation_version(client):
    create_protocol(client)
    create_site(client, "w0", ["other"])
    create_site(client, "w1", ["chapter-transfer"])
    low = client.post("/api/pilots/sessions", json=submit_payload("priority-low", priority=10)).json()
    high = client.post("/api/pilots/sessions", json=submit_payload("priority-high", priority=90)).json()
    no_match = claim(client, "w0", capabilities=["other"])
    assert no_match["claimed"] is False and no_match["session"] is None
    assert no_match["reason"] == "no_capable_task"
    claimed = claim(client, "w1")
    assert claimed["claimed"] is True
    assert claimed["session"]["id"] == high["id"]
    assert claimed["site"]["occupied"] == 1 and claimed["site"]["available_slots"] == 0
    completed = client.post(
        f"/api/pilots/sessions/{high['id']}/complete",
        json={"site_code": "w1", "observation": {"value": 3.14}, "metrics": {"seconds": 2}},
    )
    assert completed.status_code == 200
    details = client.get(f"/api/pilots/session-details/{high['id']}").json()
    assert details["status"] == "succeeded"
    assert details["current_observation_version"] == 1
    assert len(details["observations"]) == 1
    assert low["status"] == "queued"


def test_quota_cancel_retry_priority_and_batch_interventions(client):
    create_protocol(client)
    quota = client.put(
        "/api/pilots/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "limited", "max_queued": 1, "max_running": 1, "daily_submissions": 2},
    )
    assert quota.status_code == 200
    one = client.post("/api/pilots/sessions", json=submit_payload("quota-one", user="limited")).json()
    blocked = client.post("/api/pilots/sessions", json=submit_payload("quota-two", user="limited"))
    assert blocked.status_code == 409
    cancelled = client.post(f"/api/pilots/sessions/{one['id']}/cancel", json={"actor": "administrator", "reason": "项目暂停"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    retried = client.post(f"/api/pilots/sessions/{one['id']}/retry", json={"actor": "administrator", "reason": "项目恢复", "priority": 95})
    assert retried.status_code == 200 and retried.json()["priority"] == 95
    other = client.post("/api/pilots/sessions", json=submit_payload("batch-other", user="other-user")).json()
    batch = client.post(
        "/api/pilots/sessions/batch",
        json={"session_ids": [one["id"], other["id"]], "operation": "priority", "actor": "administrator", "reason": "线路合作方临时到场", "priority": 99},
    )
    assert batch.status_code == 200
    assert len(batch.json()["succeeded"]) == 2
    details = client.get(f"/api/pilots/session-details/{one['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel", "retry", "priority"]


def test_failure_backoff_and_expired_lease_recovery(client):
    from app.database import init_db

    init_db()
    create_site(client, "site-a", ["chapter-transfer"])
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    first = service.submit(submit_payload("failure-000001"))
    claimed = service.claim("site-a", ["chapter-transfer"], 10)
    assert claimed["claimed"] and claimed["session"]["id"] == first["id"]
    failed = service.fail(first["id"], "site-a", "connection_delayed", "前序列车晚点导致接驳窗口不稳定", True)
    assert failed["status"] == "queued"
    assert failed["available_at"] > failed["updated_at"]
    clock.advance(seconds=2)
    claimed_again = service.claim("site-a", ["chapter-transfer"], 10)
    assert claimed_again["claimed"] and claimed_again["session"]["attempt_count"] == 2
    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["exhausted"] == [first["id"]]
    details = service.get_session(first["id"])
    assert details["status"] == "failed"
    assert details["interventions"][-1]["action"] == "lease_recovery"



# ---------------------------------------------------------------------------
# 容量前置校验、拒绝原因与并发领取验收
# ---------------------------------------------------------------------------


def test_claim_requires_registered_site_and_leaves_queue_intact(client):
    create_protocol(client)
    submitted = client.post("/api/pilots/sessions", json=submit_payload("site-missing-1")).json()
    rejected = claim(client, "ghost-site")
    assert rejected["claimed"] is False
    assert rejected["session"] is None
    assert rejected["reason"] == "site_not_found"
    # 失败领取不允许丢项：任务仍在公共队列
    queued = client.get("/api/pilots/sessions?status=queued").json()["items"]
    assert [item["id"] for item in queued] == [submitted["id"]]


def test_claim_rejects_unavailable_site_and_capability_mismatch(client):
    create_protocol(client)
    create_site(client, "paused", ["chapter-transfer"])
    create_site(client, "mismatch", ["pottery-making"])
    client.patch("/api/catalog/sites/paused", json={"status": "suspended"})
    client.post("/api/pilots/sessions", json=submit_payload("ready-task-1"))

    suspended = claim(client, "paused")
    assert suspended["claimed"] is False and suspended["reason"] == "site_suspended"
    assert suspended["site"]["status"] == "suspended" and suspended["site"]["available_slots"] == 0

    mismatch = claim(client, "mismatch")
    assert mismatch["claimed"] is False and mismatch["reason"] == "capability_mismatch"
    assert mismatch["site"]["occupied"] == 0

    # 声明能力与节点登记能力取交集：交集为空同样拒绝，不能按声明能力越权领取
    create_site(client, "partial", ["chapter-transfer"])
    partial = claim(client, "partial", capabilities=["chapter-transfer", "night-cruise"])
    assert partial["claimed"] is True
    wrong_only = claim(client, "partial", capabilities=["night-cruise"])
    assert wrong_only["claimed"] is False and wrong_only["reason"] == "capability_mismatch"
    assert state_counts(client).get("queued", 0) == 0


def test_single_capacity_node_holds_at_most_one_and_reports_capacity_full(client):
    create_protocol(client)
    create_site(client, "rail-a", ["chapter-transfer"], max_concurrent=1)
    first = client.post("/api/pilots/sessions", json=submit_payload("cap-task-1")).json()
    second = client.post("/api/pilots/sessions", json=submit_payload("cap-task-2")).json()

    held = claim(client, "rail-a")
    assert held["claimed"] is True and held["session"]["id"] == first["id"]
    assert held["site"] == {
        "site_code": "rail-a", "status": "active", "max_concurrent": 1,
        "running": 1, "stopping": 0, "occupied": 1, "available_slots": 0,
    }

    denied = claim(client, "rail-a")
    assert denied["claimed"] is False and denied["reason"] == "capacity_full"
    assert denied["occupancy"] == {"running": 1, "stopping": 0, "occupied": 1}

    # 被拒任务必须原样留在队列中留给其他节点
    queued = [item["id"] for item in client.get("/api/pilots/sessions?status=queued").json()["items"]]
    assert queued == [second["id"]]

    # 其他有余量且能力匹配的节点可以接走该任务
    create_site(client, "rail-b", ["chapter-transfer"], max_concurrent=2)
    picked = claim(client, "rail-b")
    assert picked["claimed"] is True and picked["session"]["id"] == second["id"]

    # rail-a 自始至终只持有一个有效任务
    held_by_a = client.get("/api/pilots/sessions?status=running").json()["items"]
    assert [item for item in held_by_a if item["lease_owner"] == "rail-a"]
    assert len([item for item in held_by_a if item["lease_owner"] == "rail-a"]) == 1


def test_completion_failure_recovery_and_scaleup_release_capacity(client):
    create_protocol(client)
    create_site(client, "rel-a", ["chapter-transfer"], max_concurrent=1)

    # 完成后释放名额
    one = client.post("/api/pilots/sessions", json=submit_payload("rel-01")).json()
    two = client.post("/api/pilots/sessions", json=submit_payload("rel-02")).json()
    claim(client, "rel-a")
    assert claim(client, "rel-a")["reason"] == "capacity_full"
    completed = client.post(f"/api/pilots/sessions/{one['id']}/complete", json={"site_code": "rel-a", "observation": {"ok": True}, "metrics": {}})
    assert completed.status_code == 200
    next_claim = claim(client, "rel-a")
    assert next_claim["claimed"] is True and next_claim["session"]["id"] == two["id"]

    # 可重试失败回到队列并释放名额
    three = client.post("/api/pilots/sessions", json=submit_payload("rel-03")).json()
    failed = client.post(f"/api/pilots/sessions/{two['id']}/fail", json={"site_code": "rel-a", "error_code": "road_block", "message": "前方道路临时管制", "retryable": False})
    assert failed.status_code == 200
    freed = claim(client, "rel-a")
    assert freed["claimed"] is True and freed["session"]["id"] == three["id"]

    # 人工扩容后立即增加容量
    four = client.post("/api/pilots/sessions", json=submit_payload("rel-04")).json()
    assert claim(client, "rel-a")["reason"] == "capacity_full"
    patched = client.patch("/api/catalog/sites/rel-a", json={"max_concurrent": 2})
    assert patched.status_code == 200 and patched.json()["max_concurrent"] == 2
    expanded = claim(client, "rel-a")
    assert expanded["claimed"] is True and expanded["session"]["id"] == four["id"]
    assert expanded["site"]["max_concurrent"] == 2 and expanded["site"]["available_slots"] == 0


def test_cancel_request_keeps_slot_until_safe_acknowledgement(client):
    create_protocol(client)
    create_site(client, "stop-a", ["chapter-transfer"], max_concurrent=1)
    running_task = client.post("/api/pilots/sessions", json=submit_payload("stop-1")).json()
    waiting_task = client.post("/api/pilots/sessions", json=submit_payload("stop-2")).json()
    claim(client, "stop-a")

    cancelled = client.post(f"/api/pilots/sessions/{running_task['id']}/cancel", json={"actor": "manager", "reason": "天气原因暂停接驳"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancel_requested"

    # 等待安全停止仍然占用名额
    denied = claim(client, "stop-a")
    assert denied["reason"] == "capacity_full"
    assert denied["occupancy"] == {"running": 0, "stopping": 1, "occupied": 1}
    assert client.get("/api/pilots/sessions?status=queued").json()["items"][0]["id"] == waiting_task["id"]

    # 非持有节点不能代为确认
    wrong = client.post(f"/api/pilots/sessions/{running_task['id']}/cancel-acknowledge", json={"site_code": "stop-b"})
    assert wrong.status_code == 409

    acknowledged = client.post(f"/api/pilots/sessions/{running_task['id']}/cancel-acknowledge", json={"site_code": "stop-a"})
    assert acknowledged.status_code == 200 and acknowledged.json()["status"] == "cancelled"
    details = client.get(f"/api/pilots/session-details/{running_task['id']}").json()
    assert details["interventions"][-1]["action"] == "cancel_acknowledge"

    freed = claim(client, "stop-a")
    assert freed["claimed"] is True and freed["session"]["id"] == waiting_task["id"]


def test_expired_cancel_request_is_collected_and_releases_capacity(client):
    from app.database import init_db

    init_db()
    create_site(client, "exp-a", ["chapter-transfer"], max_concurrent=1)
    clock = FrozenClock(datetime(2026, 10, 6, 8, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    session = service.submit(submit_payload("exp-cancel-1"))
    claimed = service.claim("exp-a", ["chapter-transfer"], 30)
    assert claimed["claimed"]
    service.cancel(session["id"], "manager", "极端天气收车")
    assert service.claim("exp-a", ["chapter-transfer"], 30)["reason"] == "capacity_full"
    clock.advance(seconds=31)
    recovered = service.recover_expired()
    assert recovered["cancelled"] == [session["id"]]
    result = service.claim("exp-a", ["chapter-transfer"], 30)
    assert result["reason"] == "no_capable_task"
    assert result["site"]["occupied"] == 0


def _run_concurrently(target, count: int):
    barrier = threading.Barrier(count)
    results: list[object] = [None] * count
    errors: list[BaseException] = []

    def worker(index: int) -> None:
        try:
            barrier.wait()
            results[index] = target(index)
        except BaseException as exc:  # 收集线程内断言错误
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(index,)) for index in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert not errors, errors
    return results


def test_concurrent_claims_on_single_free_slot_only_one_wins(client):
    create_protocol(client)
    create_site(client, "sole", ["chapter-transfer"], max_concurrent=1)

    def compete(_index: int) -> dict:
        response = client.post("/api/pilots/sessions/claim", json={"site_code": "sole", "capabilities": ["chapter-transfer"], "lease_seconds": 60})
        assert response.status_code == 200, response.text
        return response.json()

    for round_index in range(5):
        client.post("/api/pilots/sessions", json=submit_payload(f"sole-task-{round_index}"))
        results = _run_concurrently(compete, 8)
        winners = [result for result in results if result["claimed"]]
        assert len(winners) == 1, results
        assert len({result["session"]["id"] for result in winners}) == 1
        held = client.get("/api/catalog/insights/sites/sole/utilization").json()
        assert held["active_sessions"] == 1 and held["over_capacity"] is False
        # 完成释放名额，下一轮才有新的空余名额可争
        completed = client.post(
            f"/api/pilots/sessions/{winners[0]['session']['id']}/complete",
            json={"site_code": "sole", "observation": {"round": round_index}, "metrics": {}},
        )
        assert completed.status_code == 200


def test_concurrent_claims_across_nodes_match_tasks_without_loss(client):
    create_protocol(client)
    task_ids = []
    for index in range(3):
        task = client.post("/api/pilots/sessions", json=submit_payload(f"race-task-{index}")).json()
        task_ids.append(task["id"])
    for index in range(6):
        create_site(client, f"node-{index}", ["chapter-transfer"], max_concurrent=1)

    def compete(index: int) -> dict:
        response = client.post("/api/pilots/sessions/claim", json={"site_code": f"node-{index}", "capabilities": ["chapter-transfer"], "lease_seconds": 60})
        assert response.status_code == 200, response.text
        return response.json()

    results = _run_concurrently(compete, 6)
    winners = [result for result in results if result["claimed"]]
    assert len(winners) == 3
    # 同一任务不可能被两个节点持有
    assert len({result["session"]["id"] for result in winners}) == 3
    assert {result["session"]["id"] for result in winners} == set(task_ids)
    # 没有超单容量持有，也没有残留在队列中的任务
    for index in range(6):
        utilization = client.get(f"/api/catalog/insights/sites/node-{index}/utilization").json()
        assert utilization["active_sessions"] <= 1
    assert state_counts(client).get("queued", 0) == 0


def test_repeated_competition_and_state_changes_keep_invariant(client):
    create_protocol(client)
    create_site(client, "knot", ["chapter-transfer"], max_concurrent=1)
    create_site(client, "relief", ["chapter-transfer"], max_concurrent=1)

    def drain_queue(site_code: str) -> list[int]:
        held: list[int] = []
        while True:
            result = claim(client, site_code)
            if not result["claimed"]:
                break
            held.append(result["session"]["id"])
        return held

    submitted: list[int] = []
    # 连续施加竞争领取，并交替触发完成、取消确认、失败，名额每轮都必须按最新状态释放
    for round_index in range(6):
        task = client.post("/api/pilots/sessions", json=submit_payload(f"invariant-{round_index}")).json()
        submitted.append(task["id"])
        results = _run_concurrently(
            lambda _i: client.post("/api/pilots/sessions/claim", json={"site_code": "knot", "capabilities": ["chapter-transfer"], "lease_seconds": 60}).json(),
            4,
        )
        winners = [result for result in results if result["claimed"]]
        assert len(winners) == 1
        knot = client.get("/api/catalog/insights/sites/knot/utilization").json()
        assert knot["active_sessions"] <= 1 and knot["over_capacity"] is False

        session_id = winners[0]["session"]["id"]
        if round_index % 3 == 0:
            response = client.post(f"/api/pilots/sessions/{session_id}/complete", json={"site_code": "knot", "observation": {"round": round_index}, "metrics": {}})
            assert response.status_code == 200
        elif round_index % 3 == 1:
            assert client.post(f"/api/pilots/sessions/{session_id}/cancel", json={"actor": "manager", "reason": "例行调度调整"}).json()["status"] == "cancel_requested"
            assert claim(client, "knot")["reason"] == "capacity_full"
            assert client.post(f"/api/pilots/sessions/{session_id}/cancel-acknowledge", json={"site_code": "knot"}).json()["status"] == "cancelled"
        else:
            response = client.post(f"/api/pilots/sessions/{session_id}/fail", json={"site_code": "knot", "error_code": "vehicle_fault", "message": "接驳车辆故障", "retryable": False})
            assert response.status_code == 200 and response.json()["status"] == "failed"

        knot = client.get("/api/catalog/insights/sites/knot/utilization").json()
        assert knot["active_sessions"] == 0 and knot["over_capacity"] is False
        # 兜底节点清空公共队列，任何残留任务都不能丢
        for leftover in drain_queue("relief"):
            response = client.post(f"/api/pilots/sessions/{leftover}/complete", json={"site_code": "relief", "observation": {"drained": True}, "metrics": {}})
            assert response.status_code == 200

    # 人工扩容到 2 后，两个排队任务在 6 路并发下恰好被领取两次，不会超发
    client.patch("/api/catalog/sites/knot", json={"max_concurrent": 2})
    scale_ids = []
    for key in ("scale-a", "scale-b"):
        scale_ids.append(client.post("/api/pilots/sessions", json=submit_payload(key)).json()["id"])
    submitted.extend(scale_ids)
    results = _run_concurrently(
        lambda _i: client.post("/api/pilots/sessions/claim", json={"site_code": "knot", "capabilities": ["chapter-transfer"], "lease_seconds": 60}).json(),
        6,
    )
    winners = [result for result in results if result["claimed"]]
    assert len(winners) == 2 and {result["session"]["id"] for result in winners} == set(scale_ids)
    knot = client.get("/api/catalog/insights/sites/knot/utilization").json()
    assert knot["active_sessions"] == 2 and knot["over_capacity"] is False

    # 全部提交任务均有归属：队列中没有因失败领取而丢失的项
    states = state_counts(client)
    assert states.get("queued", 0) == 0
    assert sum(states.values()) == len(submitted) == 8
