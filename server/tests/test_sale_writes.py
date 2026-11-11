from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from pos4africa.shared.utils.sale_ref import extract_ref, strip_ref
from server.domain.errors import NotFoundError, PosOutcomeUnknownError, PosRejectedError, ValidationFailed
from server.domain.sales.models import CreateSaleRequest
from server.domain.write_log import WriteStatus
from server.tests.conftest import CASH


def sale_req(**over) -> CreateSaleRequest:
    body = {
        "pos_customer_id": 10,
        "lines": [{"pos_item_id": 1, "quantity": 2}, {"pos_item_id": 2, "quantity": 1, "unit_price": 44000}],
        "payments": [{"account_id": str(CASH), "amount": 204000}],
        "comment": "deliver friday",
    }
    return CreateSaleRequest(**{**body, **over})


@pytest.fixture
def w(world):
    world.customers.add(10, "Aisha", "Bello")
    return world


async def test_happy_path_uses_the_real_pos_code_and_a_clean_comment(w):
    result = await w.container.create_sale.execute(sale_req(), "sale-key-1")

    assert result.status is WriteStatus.SYNCED and result.pos_id == 7441
    (code, sale), = w.sales.committed
    assert code == 7441                                   # id came from POS, not a local sequence
    assert sale.total == Decimal("204000.00")             # 2 x 80000 + 44000 override
    assert sale.lines[1].unit_price == Decimal("44000.00")
    assert sale.comment == "deliver friday"               # what goes to analytics: no ref
    sent, committed = w.pos_sales.create_calls[0]
    assert committed is True
    assert extract_ref(sent.comment_with_ref) == sale.ref
    assert strip_ref(sent.comment_with_ref) == "deliver friday"


async def test_replay_does_not_create_a_second_sale(w):
    await w.container.create_sale.execute(sale_req(), "sale-key-1")
    again = await w.container.create_sale.execute(sale_req(), "sale-key-1")
    assert again.replayed and len(w.pos_sales.create_calls) == 1 and len(w.sales.committed) == 1


async def test_walk_in_default(w):
    await w.container.create_sale.execute(sale_req(pos_customer_id=None), "sale-key-1")
    assert w.sales.committed[0][1].pos_customer_id == 23


@pytest.mark.parametrize("override, exc", [
    ({"pos_customer_id": 999}, NotFoundError),
    ({"lines": [{"pos_item_id": 99, "quantity": 1}]}, ValidationFailed),
    ({"payments": [{"account_id": str(CASH), "amount": 1000}]}, ValidationFailed),
    ({"payments": [{"account_id": str(uuid4()), "amount": 204000}]}, ValidationFailed),
])
async def test_invalid_requests_never_reach_pos(w, override, exc):
    with pytest.raises(exc):
        await w.container.create_sale.execute(sale_req(**override), "sale-key-1")
    assert w.pos_sales.create_calls == [] and w.log.rows == {}


async def test_inactive_item_is_refused(w):
    w.items.add(3, "Old stock", 1000, inactive=True)
    with pytest.raises(ValidationFailed, match="inactive"):
        await w.container.create_sale.execute(
            sale_req(lines=[{"pos_item_id": 3, "quantity": 1}], payments=[{"account_id": str(CASH), "amount": 1000}]),
            "sale-key-1",
        )


async def test_cart_rejected_by_pos_is_final(w):
    w.pos_sales.create_error = PosRejectedError("POS cart total 1 != expected 204000.00")
    w.pos_sales.lands = False
    with pytest.raises(PosRejectedError):
        await w.container.create_sale.execute(sale_req(), "sale-key-1")
    assert (await w.log.list())[0].status is WriteStatus.FAILED and w.sales.committed == []


async def test_reply_lost_but_sale_landed_is_recovered_by_reading_pos(w):
    w.pos_sales.create_error = PosOutcomeUnknownError("read timeout on complete")  # sale did land
    with pytest.raises(PosOutcomeUnknownError):
        await w.container.create_sale.execute(sale_req(), "sale-key-1")
    assert (await w.log.list())[0].status is WriteStatus.UNCERTAIN and w.sales.committed == []

    w.log.age_all()
    (entry,) = await w.container.reconciler.all_open()
    assert entry.status is WriteStatus.SYNCED and entry.pos_id == 7441
    assert [c for c, _ in w.sales.committed] == [7441]
    assert len(w.pos_sales.create_calls) == 1  # never re-sent


async def test_completed_but_unreadable_stays_uncertain_then_resolves(w):
    w.pos_sales.readable = False
    with pytest.raises(PosOutcomeUnknownError, match="could not be read back"):
        await w.container.create_sale.execute(sale_req(), "sale-key-1")
    assert (await w.log.list())[0].status is WriteStatus.UNCERTAIN

    w.pos_sales.readable = True
    (entry,) = await w.container.reconciler.all_open()
    assert entry.status is WriteStatus.SYNCED


async def test_pos_sale_with_different_contents_is_flagged_not_committed(w):
    w.pos_sales.mutate = "total"  # someone edited the sale in POS, or POS rounded differently
    with pytest.raises(PosOutcomeUnknownError, match="contents differ"):
        await w.container.create_sale.execute(sale_req(), "sale-key-1")
    assert (await w.log.list())[0].status is WriteStatus.UNCERTAIN and w.sales.committed == []


async def test_sale_provably_absent_after_lost_reply_becomes_retryable(w):
    w.pos_sales.create_error = PosOutcomeUnknownError("reset")
    w.pos_sales.lands = False
    with pytest.raises(PosOutcomeUnknownError):
        await w.container.create_sale.execute(sale_req(), "sale-key-1")
    w.log.age_all()
    (entry,) = await w.container.reconciler.all_open()
    assert entry.status is WriteStatus.FAILED and entry.retryable


async def test_dry_run_builds_the_cart_but_commits_nothing(dry_world):
    dry_world.customers.add(10, "Aisha", "Bello")
    result = await dry_world.container.create_sale.execute(sale_req(), "sale-key-1")
    assert result.dry_run
    assert dry_world.pos_sales.create_calls[0][1] is False
    assert dry_world.log.rows == {} and dry_world.sales.committed == []
