-- Drop existing tables if re-initializing
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS merchants CASCADE;

-- 1. Merchants Table
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    fee_percentage NUMERIC(5, 4) NOT NULL CHECK (fee_percentage >= 0),
    status VARCHAR(32) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- 2. Accounts Table
CREATE TABLE IF NOT EXISTS accounts (
    account_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id UUID NOT NULL REFERENCES merchants(merchant_id) ON DELETE RESTRICT,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- 3. Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id UUID NOT NULL REFERENCES merchants(merchant_id) ON DELETE RESTRICT,
    amount NUMERIC(18, 4) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- Enable full CDC logging for update/delete delta state
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- Seed initial test records
INSERT INTO merchants (merchant_id, name, fee_percentage, status)
VALUES 
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'Acme Corp', 0.0250, 'ACTIVE'),
    ('b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', 'Globex Retail', 0.0175, 'ACTIVE');

INSERT INTO accounts (account_id, merchant_id, currency)
VALUES 
    ('c2eebc99-9c0b-4ef8-bb6d-6bb9bd380a33', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'USD'),
    ('d3eebc99-9c0b-4ef8-bb6d-6bb9bd380a44', 'b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', 'USD');

INSERT INTO transactions (transaction_id, merchant_id, amount, currency, status)
VALUES 
    ('e4eebc99-9c0b-4ef8-bb6d-6bb9bd380a55', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 150.0000, 'USD', 'SETTLED');