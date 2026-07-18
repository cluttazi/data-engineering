-- Operational schema of the simulated retail-banking source system.
--
-- In a production deployment this database is the Debezium capture target:
-- wal_level=logical is set in docker-compose, and a Debezium Postgres
-- connector would publish row-level changes from these tables to Kafka.
-- The CDC simulator in ingestion/cdc_simulator emits envelopes with the
-- same shape a connector on this schema would produce, so the downstream
-- pipelines are exercised against realistic payloads either way.

CREATE SCHEMA IF NOT EXISTS banking;

CREATE TABLE banking.customers (
    customer_id   TEXT PRIMARY KEY,
    full_name     TEXT        NOT NULL,
    email         TEXT        NOT NULL,
    phone         TEXT,
    address       TEXT,
    segment       TEXT        NOT NULL, -- retail | premium | private
    risk_rating   TEXT        NOT NULL, -- low | medium | high
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE banking.accounts (
    account_id    TEXT PRIMARY KEY,
    customer_id   TEXT        NOT NULL REFERENCES banking.customers (customer_id),
    account_type  TEXT        NOT NULL, -- checking | savings | term_deposit
    currency      TEXT        NOT NULL, -- ISO 4217
    balance       NUMERIC(18, 2) NOT NULL,
    status        TEXT        NOT NULL, -- active | frozen | closed
    opened_at     TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL
);

CREATE TABLE banking.transactions (
    transaction_id TEXT PRIMARY KEY,
    account_id     TEXT        NOT NULL REFERENCES banking.accounts (account_id),
    amount         NUMERIC(18, 2) NOT NULL,
    currency       TEXT        NOT NULL,
    txn_type       TEXT        NOT NULL, -- deposit | withdrawal | transfer | card_payment | fee
    counterparty   TEXT,
    channel        TEXT        NOT NULL, -- branch | atm | online | mobile
    status         TEXT        NOT NULL, -- posted | pending | reversed
    booked_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE banking.loan_applications (
    application_id TEXT PRIMARY KEY,
    customer_id    TEXT        NOT NULL REFERENCES banking.customers (customer_id),
    product        TEXT        NOT NULL, -- personal_loan | mortgage | auto_loan | card
    amount         NUMERIC(18, 2) NOT NULL,
    term_months    INTEGER     NOT NULL,
    status         TEXT        NOT NULL, -- submitted | under_review | approved | rejected
    credit_score   INTEGER,
    submitted_at   TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

-- Publication a real Debezium connector would use.
CREATE PUBLICATION banking_cdc FOR TABLES IN SCHEMA banking;
