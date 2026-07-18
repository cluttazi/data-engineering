"""Stateful synthetic retail-banking domain.

``BankingDomain`` owns the mutable registries (customers, accounts, loan
applications) so that update/delete events carry a faithful ``before`` image,
exactly as logical replication would. All randomness flows through a single
seeded ``random.Random`` plus a seeded ``Faker`` instance, and event time comes
from a seeded logical clock — two runs with the same seed produce byte-identical
output, which the tests and the demo's row-count assertions rely on.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime
from typing import Any

from faker import Faker

# Fixed epoch for the logical clock: 2026-01-01T00:00:00Z. Wall time never
# leaks into events, keeping runs reproducible.
CLOCK_EPOCH_MS = 1_767_225_600_000

CUSTOMER_SEGMENTS = ["retail", "retail", "retail", "premium", "private"]
RISK_RATINGS = ["low", "low", "low", "medium", "medium", "high"]
ACCOUNT_TYPES = ["checking", "checking", "savings", "term_deposit"]
ACCOUNT_STATUSES = ["active", "frozen", "closed"]
CURRENCIES = ["JPY", "JPY", "JPY", "USD", "EUR"]
TXN_TYPES = ["card_payment", "card_payment", "transfer", "deposit", "withdrawal", "fee"]
CHANNELS = ["mobile", "mobile", "online", "atm", "branch"]
TXN_STATUSES = ["posted", "posted", "posted", "pending", "reversed"]
LOAN_PRODUCTS = ["personal_loan", "mortgage", "auto_loan", "card"]
LOAN_TRANSITIONS: dict[str, list[str]] = {
    "submitted": ["under_review"],
    "under_review": ["approved", "approved", "rejected"],
}

Row = dict[str, Any]


class LogicalClock:
    """Deterministic event-time source advancing by a seeded random stride."""

    def __init__(self, rng: random.Random, epoch_ms: int = CLOCK_EPOCH_MS) -> None:
        self._rng = rng
        self._now_ms = epoch_ms

    def tick(self) -> int:
        """Advance 100ms-5s and return the new timestamp in epoch millis."""
        self._now_ms += self._rng.randint(100, 5_000)
        return self._now_ms

    @property
    def now_ms(self) -> int:
        return self._now_ms


class BankingDomain:
    """Registries + mutation logic for the four CDC-tracked entities."""

    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.faker = Faker()
        self.faker.seed_instance(seed)
        self.clock = LogicalClock(self.rng)
        self.customers: dict[str, Row] = {}
        self.accounts: dict[str, Row] = {}
        self.loan_applications: dict[str, Row] = {}
        self._counters = {"customer": 0, "account": 0, "transaction": 0, "loan": 0}

    def _next_id(self, kind: str, prefix: str) -> str:
        self._counters[kind] += 1
        return f"{prefix}-{self._counters[kind]:07d}"

    def _iso(self, ts_ms: int) -> str:
        # Debezium emits epoch micros for TIMESTAMPTZ; we keep ISO-8601 strings
        # in payloads for readability and parse them downstream with try_cast.
        return datetime.fromtimestamp(ts_ms / 1000, tz=UTC).isoformat(timespec="milliseconds")

    # -- customers ---------------------------------------------------------

    def insert_customer(self) -> Row:
        ts = self.clock.tick()
        customer_id = self._next_id("customer", "CUST")
        row: Row = {
            "customer_id": customer_id,
            "full_name": self.faker.name(),
            "email": self.faker.email(),
            "phone": self.faker.phone_number(),
            "address": self.faker.address().replace("\n", ", "),
            "segment": self.rng.choice(CUSTOMER_SEGMENTS),
            "risk_rating": self.rng.choice(RISK_RATINGS),
            "created_at": self._iso(ts),
            "updated_at": self._iso(ts),
        }
        self.customers[customer_id] = row
        return row

    def update_customer(self) -> tuple[Row, Row] | None:
        """Mutate a random customer; returns (before, after) or None if empty."""
        if not self.customers:
            return None
        customer_id = self.rng.choice(sorted(self.customers))
        before = dict(self.customers[customer_id])
        after = dict(before)
        mutation = self.rng.choice(["email", "phone", "address", "segment", "risk_rating"])
        if mutation == "email":
            after["email"] = self.faker.email()
        elif mutation == "phone":
            after["phone"] = self.faker.phone_number()
        elif mutation == "address":
            after["address"] = self.faker.address().replace("\n", ", ")
        elif mutation == "segment":
            after["segment"] = self.rng.choice(CUSTOMER_SEGMENTS)
        else:
            after["risk_rating"] = self.rng.choice(RISK_RATINGS)
        after["updated_at"] = self._iso(self.clock.tick())
        self.customers[customer_id] = after
        return before, after

    # -- accounts ----------------------------------------------------------

    def insert_account(self) -> Row | None:
        if not self.customers:
            return None
        ts = self.clock.tick()
        account_id = self._next_id("account", "ACCT")
        row: Row = {
            "account_id": account_id,
            "customer_id": self.rng.choice(sorted(self.customers)),
            "account_type": self.rng.choice(ACCOUNT_TYPES),
            "currency": self.rng.choice(CURRENCIES),
            "balance": round(self.rng.uniform(0, 5_000_000), 2),
            "status": "active",
            "opened_at": self._iso(ts),
            "updated_at": self._iso(ts),
        }
        self.accounts[account_id] = row
        return row

    def update_account(self) -> tuple[Row, Row] | None:
        if not self.accounts:
            return None
        account_id = self.rng.choice(sorted(self.accounts))
        before = dict(self.accounts[account_id])
        after = dict(before)
        if self.rng.random() < 0.85:  # balance movement is the common case
            delta = round(self.rng.uniform(-200_000, 300_000), 2)
            after["balance"] = round(max(0.0, float(before["balance"]) + delta), 2)
        else:
            after["status"] = self.rng.choice(ACCOUNT_STATUSES)
        after["updated_at"] = self._iso(self.clock.tick())
        self.accounts[account_id] = after
        return before, after

    # -- transactions (append-only) ---------------------------------------

    def insert_transaction(self) -> Row | None:
        if not self.accounts:
            return None
        ts = self.clock.tick()
        row: Row = {
            "transaction_id": self._next_id("transaction", "TXN"),
            "account_id": self.rng.choice(sorted(self.accounts)),
            "amount": round(self.rng.uniform(100, 800_000), 2),
            "currency": self.rng.choice(CURRENCIES),
            "txn_type": self.rng.choice(TXN_TYPES),
            "counterparty": self.faker.company(),
            "channel": self.rng.choice(CHANNELS),
            "status": self.rng.choice(TXN_STATUSES),
            "booked_at": self._iso(ts),
        }
        return row

    # -- loan applications -------------------------------------------------

    def insert_loan_application(self) -> Row | None:
        if not self.customers:
            return None
        ts = self.clock.tick()
        application_id = self._next_id("loan", "LOAN")
        row: Row = {
            "application_id": application_id,
            "customer_id": self.rng.choice(sorted(self.customers)),
            "product": self.rng.choice(LOAN_PRODUCTS),
            "amount": round(self.rng.uniform(100_000, 30_000_000), 2),
            "term_months": self.rng.choice([12, 24, 36, 60, 120, 240, 360]),
            "status": "submitted",
            "credit_score": self.rng.randint(300, 850),
            "submitted_at": self._iso(ts),
            "updated_at": self._iso(ts),
        }
        self.loan_applications[application_id] = row
        return row

    def advance_loan_application(self) -> tuple[Row, Row] | None:
        """Move one non-terminal application through its status workflow."""
        open_apps = sorted(
            app_id
            for app_id, row in self.loan_applications.items()
            if row["status"] in LOAN_TRANSITIONS
        )
        if not open_apps:
            return None
        application_id = self.rng.choice(open_apps)
        before = dict(self.loan_applications[application_id])
        after = dict(before)
        after["status"] = self.rng.choice(LOAN_TRANSITIONS[str(before["status"])])
        after["updated_at"] = self._iso(self.clock.tick())
        self.loan_applications[application_id] = after
        return before, after
