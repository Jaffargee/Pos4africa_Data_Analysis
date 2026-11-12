from __future__ import annotations

import asyncio

import pytest

from server.domain.customers.models import CustomerCreateRequest, CustomerUpdateRequest
from server.domain.errors import ConflictError, PosOutcomeUnknownError, PosRejectedError, PosUnavailableError
from server.domain.write_log import WriteStatus

REQ = CustomerCreateRequest(first_name="Aisha", last_name="Bello", phone_number="0803 123 4567", state="Kano")


async def test_create_happy_path_records_the_pos_id(world):
    result = await world.container.create_customer.execute(REQ, "key-0001")

    assert result.status is WriteStatus.POSTED and result.pos_id == 501 and not result.replayed
    assert len(world.pos_customers.create_calls) == 1
    assert world.customers.rows == {}  # the server never writes to Supabase; the ETL will


async def test_same_key_replays_without_touching_pos_again(world):
    first = await world.container.create_customer.execute(REQ, "key-0001")
    second = await world.container.create_customer.execute(REQ, "key-0001")

    assert second.replayed and second.pos_id == first.pos_id
    assert len(world.pos_customers.create_calls) == 1


async def test_same_key_different_body_is_refused(world):
    await world.container.create_customer.execute(REQ, "key-0001")
    other = CustomerCreateRequest(first_name="Musa", allow_duplicate=True)
    with pytest.raises(ConflictError, match="different request"):
        await world.container.create_customer.execute(other, "key-0001")


async def test_concurrent_duplicates_send_exactly_once(world):
    results = await asyncio.gather(
        world.container.create_customer.execute(REQ, "key-0001"),
        world.container.create_customer.execute(REQ, "key-0001"),
        return_exceptions=True,
    )
    assert len(world.pos_customers.create_calls) == 1
    assert sum(isinstance(r, ConflictError) for r in results) == 1
    assert sum(not isinstance(r, Exception) for r in results) == 1


async def test_pos_rejection_is_final_and_replayed_without_resending(world):
    world.pos_customers.outcomes = [PosRejectedError("duplicate phone")]
    with pytest.raises(PosRejectedError):
        await world.container.create_customer.execute(REQ, "key-0001")
    entry = (await world.log.list())[0]
    assert entry.status is WriteStatus.FAILED and entry.retryable is False

    with pytest.raises(PosRejectedError, match="duplicate phone"):
        await world.container.create_customer.execute(REQ, "key-0001")
    assert len(world.pos_customers.create_calls) == 1


async def test_unreachable_pos_is_retryable_with_the_same_key(world):
    world.pos_customers.outcomes = [PosUnavailableError("down")]
    with pytest.raises(PosUnavailableError):
        await world.container.create_customer.execute(REQ, "key-0001")
    entry = (await world.log.list())[0]
    assert entry.status is WriteStatus.FAILED and entry.retryable is True

    result = await world.container.create_customer.execute(REQ, "key-0001")
    assert result.status is WriteStatus.POSTED
    assert (await world.log.list())[0].attempts == 2


async def test_lost_reply_never_blind_retries_and_reconciles_to_the_real_id(world):
    world.pos_customers.outcomes = [PosOutcomeUnknownError("read timeout")]
    with pytest.raises(PosOutcomeUnknownError):
        await world.container.create_customer.execute(REQ, "key-0001")
    assert (await world.log.list())[0].status is WriteStatus.UNCERTAIN

    with pytest.raises(ConflictError, match="UNCERTAIN"):  # resending is blocked, not repeated
        await world.container.create_customer.execute(REQ, "key-0001")

    world.pos_customers.find_result = 777  # it did land in POS
    world.log.age_all()
    done = await world.container.reconciler.all_open()

    assert [e.status for e in done] == [WriteStatus.POSTED] and done[0].pos_id == 777
    assert len(world.pos_customers.create_calls) == 1  # still exactly one send


async def test_lost_reply_that_provably_did_not_land_becomes_retryable(world):
    world.pos_customers.outcomes = [PosOutcomeUnknownError("reset")]
    with pytest.raises(PosOutcomeUnknownError):
        await world.container.create_customer.execute(REQ, "key-0001")

    world.pos_customers.find_result = None
    world.log.age_all()
    (entry,) = await world.container.reconciler.all_open()
    assert entry.status is WriteStatus.FAILED and entry.retryable

    result = await world.container.create_customer.execute(REQ, "key-0001")
    assert result.status is WriteStatus.POSTED and len(world.pos_customers.create_calls) == 2


async def test_dry_run_rehearses_but_logs_nothing(dry_world):
    result = await dry_world.container.create_customer.execute(REQ, "key-0001")
    assert result.dry_run and result.status is None
    assert dry_world.pos_customers.create_calls[0][1] is False  # commit=False
    assert dry_world.log.rows == {}


async def test_duplicate_name_guard(world):
    world.customers.add(10, "Aisha", "Bello")
    with pytest.raises(ConflictError, match="already exists"):
        await world.container.create_customer.execute(REQ, "key-0001")

    ok = CustomerCreateRequest(**{**REQ.model_dump(), "allow_duplicate": True})
    assert (await world.container.create_customer.execute(ok, "key-0002")).status is WriteStatus.POSTED


async def test_update_unknown_outcome_is_retryable_not_uncertain(world):
    world.customers.add(10, "Aisha", "Bello")
    world.pos_customers.outcomes = [PosOutcomeUnknownError("timeout")]
    req = CustomerUpdateRequest(state="Lagos")
    with pytest.raises(PosOutcomeUnknownError):
        await world.container.update_customer.execute(10, req, "key-0001")
    entry = (await world.log.list())[0]
    assert entry.status is WriteStatus.FAILED and entry.retryable  # updates are idempotent

    result = await world.container.update_customer.execute(10, req, "key-0001")
    assert result.status is WriteStatus.POSTED and len(world.pos_customers.update_calls) == 2
    assert world.pos_customers.update_calls[-1][1].state == "Lagos"


def test_request_validation():
    with pytest.raises(ValueError):
        CustomerCreateRequest(first_name="  ")
    with pytest.raises(ValueError):
        CustomerCreateRequest(first_name="A", phone_number="123")
    with pytest.raises(ValueError):
        CustomerUpdateRequest()
    with pytest.raises(ValueError):
        CustomerCreateRequest(first_name="A", bogus="x")
    assert CustomerCreateRequest(first_name=" Ai  sha ").first_name == "Ai sha"
