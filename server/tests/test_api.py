from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from server.domain.errors import PosOutcomeUnknownError, PosRejectedError, PosUnavailableError
from server.main import create_app
from server.tests.conftest import API_KEY, CASH, World

AUTH = {"X-API-Key": API_KEY}
WRITE = {**AUTH, "Idempotency-Key": "client-key-0001"}
CUSTOMER = {"first_name": "Aisha", "last_name": "Bello", "state": "Kano"}


@pytest.fixture
def api(world):
    return TestClient(create_app(world.container)), world


def test_health_is_open_and_reports_mode(api):
    client, _ = api
    assert client.get("/health").json() == {"status": "ok", "pos_write_mode": "commit"}


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
def test_every_data_route_requires_the_api_key(api, headers):
    client, _ = api
    for method, path in [("post", "/customers"), ("patch", "/customers/10"), ("post", "/sales"),
                         ("get", "/recon"), ("post", "/recon/reconcile")]:
        assert getattr(client, method)(path, headers=headers).status_code == 401, path


def test_writes_require_an_idempotency_key(api):
    client, _ = api
    r = client.post("/customers", json=CUSTOMER, headers=AUTH)
    assert r.status_code == 400 and "Idempotency-Key" in r.json()["detail"]
    r = client.post("/customers", json=CUSTOMER, headers={**AUTH, "Idempotency-Key": "short"})
    assert r.status_code == 400


def test_create_then_replay(api):
    client, w = api
    first = client.post("/customers", json=CUSTOMER, headers=WRITE)
    assert first.status_code == 201
    assert first.json()["status"] == "POSTED" and first.json()["pos_id"] == 501

    again = client.post("/customers", json=CUSTOMER, headers=WRITE)
    assert again.status_code == 200 and again.json()["replayed"] is True
    assert len(w.pos_customers.create_calls) == 1


def test_validation_errors_are_422(api):
    client, _ = api
    assert client.post("/customers", json={"first_name": ""}, headers=WRITE).status_code == 422
    assert client.post("/customers", json={**CUSTOMER, "nope": 1}, headers=WRITE).status_code == 422


@pytest.mark.parametrize("error, status, code", [
    (PosRejectedError("no"), 422, "pos_rejected"),
    (PosUnavailableError("down"), 503, "pos_unavailable"),
    (PosOutcomeUnknownError("lost"), 502, "pos_outcome_unknown"),
])
def test_pos_failures_map_to_distinct_statuses(api, error, status, code):
    client, w = api
    w.pos_customers.outcomes = [error]
    r = client.post("/customers", json=CUSTOMER, headers=WRITE)
    assert r.status_code == status and r.json()["error"] == code


def test_unknown_outcome_blocks_resend_until_reconciled(api):
    client, w = api
    w.pos_customers.outcomes = [PosOutcomeUnknownError("lost")]
    assert client.post("/customers", json=CUSTOMER, headers=WRITE).status_code == 502
    assert client.post("/customers", json=CUSTOMER, headers=WRITE).status_code == 409

    (entry,) = client.get("/recon", params={"status": "UNCERTAIN"}, headers=AUTH).json()
    w.pos_customers.find_result = 900
    r = client.post(f"/recon/{entry['ref']}/reconcile", headers=AUTH)
    assert r.status_code == 200 and r.json()["status"] == "POSTED" and r.json()["pos_id"] == 900
    assert client.get(f"/recon/{entry['ref']}", headers=AUTH).json()["status"] == "POSTED"
    assert client.get("/recon/NOPE", headers=AUTH).status_code == 404


def test_dry_run_mode_returns_200_and_writes_nothing():
    w = World(mode="dry_run")
    client = TestClient(create_app(w.container))
    r = client.post("/customers", json=CUSTOMER, headers=WRITE)
    assert r.status_code == 200 and r.json()["dry_run"] is True and r.json()["status"] is None
    assert w.log.rows == {}


def test_sale_end_to_end_over_http(api):
    client, w = api
    w.customers.add(10, "Aisha", "Bello")
    body = {"pos_customer_id": 10, "lines": [{"pos_item_id": 1, "quantity": 1}],
            "payments": [{"account_id": str(CASH), "amount": 80000}]}
    r = client.post("/sales", json=body, headers={**AUTH, "Idempotency-Key": "sale-key-0001"})
    assert r.status_code == 201 and r.json()["pos_id"] == 7441

    bad = {**body, "payments": [{"account_id": str(CASH), "amount": 5}]}
    r = client.post("/sales", json=bad, headers={**AUTH, "Idempotency-Key": "sale-key-0002"})
    assert r.status_code == 422 and r.json()["error"] == "validation_failed"


def test_the_server_exposes_no_read_endpoints_the_dashboard_reads_supabase(api):
    client, w = api
    w.customers.add(10, "Aisha", "Bello")
    for path in ("/customers", "/customers/10", "/sales", "/sales/1", "/items"):
        assert client.get(path, headers=AUTH).status_code in (404, 405), path


def test_patch_customer(api):
    client, w = api
    w.customers.add(10, "Aisha", "Bello")
    r = client.patch("/customers/10", json={"state": "Lagos"}, headers=WRITE)
    assert r.status_code == 201 or r.status_code == 200
    assert w.pos_customers.update_calls[0][0] == 10 and w.pos_customers.update_calls[0][1].state == "Lagos"
    assert client.patch("/customers/10", json={}, headers=WRITE).status_code == 422
    assert client.patch("/customers/99", json={"state": "x"}, headers={**AUTH, "Idempotency-Key": "client-key-0002"}).status_code == 404
