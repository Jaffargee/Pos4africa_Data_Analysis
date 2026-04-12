from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class JobStatus(StrEnum):
      PENDING  = "pending"
      RUNNING  = "running"
      DONE     = "done"
      FAILED   = "failed"
      RETRYING = "retrying"


class ScrapeJob(BaseModel):
      """One unit of work: fetch + scrape + parse + process ONE sale."""
      pos_sale_id: int
      node_id:     str | None   = None
      status:      JobStatus    = JobStatus.PENDING
      attempt:     int          = 0
      max_retries: int          = 3
      created_at:  datetime     = Field(default_factory=datetime.utcnow)
      started_at:  datetime | None = None
      completed_at: datetime | None = None
      error:       str | None   = None

      def mark_started(self) -> None:
            self.status     = JobStatus.RUNNING
            self.started_at = datetime.utcnow()
            self.attempt   += 1

      def mark_done(self) -> None:
            self.status       = JobStatus.DONE
            self.completed_at = datetime.utcnow()

      def mark_failed(self, error: str) -> None:
            self.error        = error
            self.completed_at = datetime.utcnow()
            self.status       = JobStatus.RETRYING if self.attempt < self.max_retries else JobStatus.FAILED


class BatchJob(BaseModel):
      """A batch of ScrapeJobs assigned to one worker node."""
      node_id:      str | None  = None
      pos_sale_ids: list[ScrapeJob]          # fixed typo: was pos_sales_id
      status:       JobStatus   = JobStatus.PENDING
      created_at:   datetime    = Field(default_factory=datetime.utcnow)
      started_at:   datetime | None = None
      completed_at: datetime | None = None
      error:        str | None  = None

      def mark_started(self) -> None:
            self.status     = JobStatus.RUNNING
            self.started_at = datetime.utcnow()

      def mark_done(self) -> None:
            self.status       = JobStatus.DONE
            self.completed_at = datetime.utcnow()

      def mark_failed(self, error: str) -> None:
            self.error        = error
            self.status       = JobStatus.FAILED
            self.completed_at = datetime.utcnow()