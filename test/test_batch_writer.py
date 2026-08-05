# tests/test_batch_writer.py
import pytest
from types import SimpleNamespace

import pos4africa.manager.egress.batch_writer as bw_mod
from pos4africa.manager.egress.batch_writer import BatchWriter
from pos4africa.config import settings


class FakeResult:
      def __init__(self, data=None, error=None):
            self.data = data or []
            self.error = error


class FakeTable:
      def __init__(self, name, recorder):
            self.name = name
            self._recorder = recorder
            self._last_payload = None
            self._op = None

      def upsert(self, payload, on_conflict=None):
            self._op = "upsert"
            self._last_payload = payload
            self._recorder.append(("upsert", self.name, payload, on_conflict))
            return self

      def insert(self, payload):
            self._op = "insert"
            self._last_payload = payload
            self._recorder.append(("insert", self.name, payload))
            return self

      def delete(self):
            self._op = "delete"
            self._recorder.append(("delete", self.name))
            return self

      def select(self, cols):
            self._op = "select"
            self._recorder.append(("select", self.name, cols))
            return self

      def in_(self, col, values):
            # chainable stub
            self._recorder.append(("in_", self.name, col, values))
            return self

      def execute(self):
            # Return a FakeResult with data mirroring the last payload for upsert/insert
            if self._op in ("upsert", "insert"):
                  # _last_payload could be list/dict
                  data = self._last_payload if self._last_payload is not None else []
                  return FakeResult(data=list(data) if isinstance(data, list) else [data])
            return FakeResult(data=[])


class FakeClient:
      def __init__(self, recorder):
            self._recorder = recorder

      def table(self, name):
            return FakeTable(name, self._recorder)


@pytest.mark.asyncio
async def test_batch_writer_chunks_and_calls_supabase(monkeypatch):
      recorder = []
      fake_client = FakeClient(recorder)
      monkeypatch.setattr(bw_mod, "spb_client", fake_client)

      # make batch size small to force multiple chunks
      monkeypatch.setattr(settings, "supabase_batch_size", 2)

      writer = BatchWriter()

      # create 5 fake records (simple dicts)
      records = [{"pos_sale_id": i, "invoice_total": 1.0, "invoice_datetime": "2023-01-01T00:00:00", "hash": "h"} for i in range(5)]
      total = await writer.write(records)

      assert total == 5
      # Ensure multiple upsert calls were recorded (sales upsert and deletes for items/payments per chunk)
      assert any(rec[0] == "upsert" and rec[1] == settings.supabase_table_sales for rec in recorder)

      # test that _raise_on_error raises when result.error is set
      bad_result = FakeResult(data=[], error="boom")
      with pytest.raises(RuntimeError):
            writer._raise_on_error(bad_result, "sales")