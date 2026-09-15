import duckdb

con = duckdb.connect(database=":memory:")
con.execute("SET extension_directory = 'D:/Data Analytics Projects/fintech-cdc-lakehouse/.duckdb_extensions';")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL delta; LOAD delta;")

# Inject S3 environment configuration directly into the DuckDB kernel
con.execute("""
    CREATE SECRET minio_delta_link (
        TYPE S3,
        KEY_ID 'admin',
        SECRET 'password123',
        ENDPOINT 'localhost:9000',
        REGION 'us-east-1',
        URL_STYLE 'path',
        USE_SSL 'false'
    );
""")

gold_query = """
    SELECT * 
    FROM delta_scan('s3://lakehouse/gold/merchant_daily_summary')
"""

gold_df = con.execute(gold_query).df()
print("PRINTING GOLD:")
print(gold_df)

silver_query = """
    SELECT * 
    FROM delta_scan('s3://lakehouse/silver/transactions')
"""
silver_df = con.execute(silver_query).df()
print("prinnting silver:")
print(silver_df)
