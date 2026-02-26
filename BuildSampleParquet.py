import duckdb
from pathlib import Path

out_dir = Path("data/datasets/demo")
out_dir.mkdir(parents=True, exist_ok=True)
out_path = out_dir / "sample.parquet"

con = duckdb.connect()
con.execute("""
COPY (
  SELECT * FROM (VALUES
    (1376609297, 1376609297, 'T1019', '2025-01', 10, 100, 1234.56),
    (1376609297, 1376609297, '99214', '2025-02', 20, 200, 9876.54),
    (1234567890, 1234567890, 'H2016', '2025-03',  5,  50,  321.00)
  ) AS t(
    BILLING_PROVIDER_NPI_NUM,
    SERVICING_PROVIDER_NPI_NUM,
    HCPCS_CODE,
    CLAIM_FROM_MONTH,
    TOTAL_UNIQUE_BENEFICIARIES,
    TOTAL_CLAIMS,
    TOTAL_PAID
  )
) TO ? (FORMAT 'parquet');
""", [str(out_path)])

print("Wrote", out_path)
