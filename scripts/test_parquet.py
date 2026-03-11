import duckdb

df = duckdb.query(
    "select * from read_parquet('data/datasets/demo/sample.parquet') limit 5"
).df()

print(df)
