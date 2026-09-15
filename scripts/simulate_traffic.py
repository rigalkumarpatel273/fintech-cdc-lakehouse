import uuid
import random
from datetime import datetime, timezone
import psycopg2

# Database connection parameters (matching docker-compose)
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "ledger_db",
    "user": "ledger_admin",
    "password": "ledger_secret"
}

# Merchant IDs from your seed data
MERCHANT_IDS = [
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",  # Acme Corp
    "b1eebc99-9c0b-4ef8-bb6d-6bb9bd380a22",  # Globex
    "c2eebc99-9c0b-4ef8-bb6d-6bb9bd380a33"   # Initech
]

CURRENCIES = ["USD", "EUR", "GBP"]
STATUSES = ["SETTLED", "SETTLED", "SETTLED", "REFUNDED"]

TOTAL_RECORDS = 500
POISON_PILL_PERCENTAGE = 0.10  # 10% invalid rows to test Quarantine

print(f"Connecting to PostgreSQL to generate {TOTAL_RECORDS} transactions...")
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# Ensure merchants exist so foreign keys don't reject valid rows
for m_id in MERCHANT_IDS:
    cur.execute("""
        INSERT INTO merchants (merchant_id, name, fee_percentage, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (merchant_id) DO NOTHING;
    """, (m_id, f"Merchant-{m_id[:6]}", 0.025))

records = []
poison_count = 0

for _ in range(TOTAL_RECORDS):
    tx_id = str(uuid.uuid4())
    merchant_id = random.choice(MERCHANT_IDS)
    currency = random.choice(CURRENCIES)
    status = random.choice(STATUSES)
    created_at = datetime.now(timezone.utc)

    # Inject 10% poison pills (negative or zero amount)
    if random.random() < POISON_PILL_PERCENTAGE:
        amount = round(random.uniform(-500.0, 0.0), 2)
        poison_count += 1
    else:
        amount = round(random.uniform(10.0, 750.0), 2)

    records.append((tx_id, merchant_id, amount, currency, status, created_at, created_at))

# Batch insert into PostgreSQL
insert_query = """
    INSERT INTO transactions (
        transaction_id, merchant_id, amount, currency, status, created_at, updated_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

cur.executemany(insert_query, records)
conn.commit()
cur.close()
conn.close()

print(f"Successfully inserted {TOTAL_RECORDS} rows into PostgreSQL.")
print(f"Target distribution -> Valid: {TOTAL_RECORDS - poison_count} | Poison pills: {poison_count}")