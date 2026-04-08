"""
publish_d1_reports — D+1 report publication.

Checks the DQ gate result in dbo.ADWPM_DQ_RESULTS, then exports the three
DW tables to MinIO (S3-compatible) as Parquet/CSV files:

  derivatives/ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO_{date}.parquet
  derivatives/ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL_{date}.parquet
  spot/ADWPM_POSICAO_MERCADO_A_VISTA_{date}.csv

conn parameter = pymssql connection to SQL Server DW (demopoc database).
"""

import io
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio


MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT",   "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET    = "mock-exchange"


def _get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def _upload_parquet(client, df, object_name):
    """Convert DataFrame to Parquet bytes and upload to MinIO."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    client.put_object(
        MINIO_BUCKET,
        object_name,
        buf,
        length=buf.getbuffer().nbytes,
        content_type="application/octet-stream",
    )


def _upload_csv(client, df, object_name):
    """Convert DataFrame to CSV bytes and upload to MinIO."""
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    client.put_object(
        MINIO_BUCKET,
        object_name,
        buf,
        length=buf.getbuffer().nbytes,
        content_type="text/csv",
    )


def run(conn, business_date):
    """
    Publish D+1 reports after verifying quality gate passed.

    Args:
        conn: pymssql connection to SQL Server DW (demopoc database)
        business_date: str in YYYY-MM-DD format

    Returns:
        dict with status and files_exported list
    """
    cur = conn.cursor(as_dict=True)

    # =========================================================================
    # Guard: skip publication if any critical DQ check failed
    # =========================================================================
    cur.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM dbo.ADWPM_DQ_RESULTS
        WHERE business_date = %s
          AND severity = 'critical'
          AND passed = 0
        """,
        (business_date,),
    )
    row = cur.fetchone()
    failed_checks = row["cnt"] if row else 0

    if failed_checks > 0:
        print(
            f"[publish] Skipping publication: {failed_checks} critical DQ check(s) "
            f"failed for {business_date}",
            flush=True,
        )
        cur.close()
        return {
            "status": "failed",
            "reason": (
                f"Quality gate failed: {failed_checks} critical check(s) did not pass "
                f"for {business_date}"
            ),
            "files_exported": [],
        }

    # =========================================================================
    # Export 1: ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO → Parquet
    # =========================================================================
    cur.execute(
        """
        SELECT trade_id, ticker, notional, quantity,
               trade_dt, settlement_dt, counterparty_id,
               trader_id, desk_code, status,
               dw_load_dt, dw_source
        FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
        WHERE trade_dt = %s
        """,
        (business_date,),
    )
    mvmt_rows = cur.fetchall()

    # =========================================================================
    # Export 2: ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL → Parquet
    # =========================================================================
    cur.execute(
        """
        SELECT position_id, instrument_id, participant_code, position_date,
               long_qty, short_qty, net_qty, settlement_price, margin_value,
               dw_load_dt, dw_source
        FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
        WHERE position_date = %s
        """,
        (business_date,),
    )
    drvt_rows = cur.fetchall()

    # =========================================================================
    # Export 3: ADWPM_POSICAO_MERCADO_A_VISTA → CSV
    # =========================================================================
    cur.execute(
        """
        SELECT position_id, ticker, participant_code, position_date,
               long_value, short_value, net_value,
               dw_load_dt, dw_source
        FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA
        WHERE position_date = %s
        """,
        (business_date,),
    )
    vista_rows = cur.fetchall()

    cur.close()

    # =========================================================================
    # Upload to MinIO
    # =========================================================================
    client = _get_minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    files_exported = []
    date_tag = business_date.replace("-", "")

    if mvmt_rows:
        df = pd.DataFrame(mvmt_rows)
        obj = f"derivatives/ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO_{date_tag}.parquet"
        _upload_parquet(client, df, obj)
        files_exported.append(obj)
        print(f"[publish] exported {len(mvmt_rows)} rows → {obj}", flush=True)
    else:
        print(
            f"[publish] ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO: no rows for {business_date}",
            flush=True,
        )

    if drvt_rows:
        df = pd.DataFrame(drvt_rows)
        obj = f"derivatives/ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL_{date_tag}.parquet"
        _upload_parquet(client, df, obj)
        files_exported.append(obj)
        print(f"[publish] exported {len(drvt_rows)} rows → {obj}", flush=True)
    else:
        print(
            f"[publish] ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL: no rows for {business_date}",
            flush=True,
        )

    if vista_rows:
        df = pd.DataFrame(vista_rows)
        obj = f"spot/ADWPM_POSICAO_MERCADO_A_VISTA_{date_tag}.csv"
        _upload_csv(client, df, obj)
        files_exported.append(obj)
        print(f"[publish] exported {len(vista_rows)} rows → {obj}", flush=True)
    else:
        print(
            f"[publish] ADWPM_POSICAO_MERCADO_A_VISTA: no rows for {business_date}",
            flush=True,
        )

    print(
        f"[publish] publication complete for {business_date}: "
        f"{len(files_exported)} file(s) exported",
        flush=True,
    )

    return {
        "status": "success",
        "files_exported": files_exported,
    }
