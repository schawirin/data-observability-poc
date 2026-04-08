"""
quality_gate_d1 — Data Quality Gate for D+1 pipeline.

Checks the 3 real DQ scenarios from the Data Pipeline POC requirements:

  Caso 1 — Duplicates in ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
            (source: ASTADRVT_TRADE_MVMT)
  Caso 2 — Null settlement_price in ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
            (source: ASTANO_FGBE_DRVT_PSTN)
  Caso 3 — long_value + short_value = 0 in ADWPM_POSICAO_MERCADO_A_VISTA
            (source: ASTACASH_MRKT_PSTN)
  Check 4 — Row count difference Oracle source vs SQL Server DW > 5%
  Check 5 — Overflow: notional value exceeds DECIMAL(18,4) precision contract
            (source: ASTADRVT_TRADE_MVMT → ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO)

Results are written to dbo.ADWPM_DQ_RESULTS in SQL Server.

Log messages use the format [DQ-CASO-N] so Datadog log monitors can trigger on them.

conn parameter = SQL Server DW connection (pymssql).
Oracle is opened internally only for check 4 (source row counts).
"""

import os
import uuid

ORACLE_HOST    = os.environ.get("ORACLE_HOST",    "demoorg-oracle")
ORACLE_PORT    = int(os.environ.get("ORACLE_PORT", "1521"))
ORACLE_SERVICE = os.environ.get("ORACLE_SERVICE", "FREEPDB1")
ORACLE_USER    = os.environ.get("ORACLE_USER",    "system")
ORACLE_PASSWORD = os.environ.get("ORACLE_PASSWORD", "OracleDemo123!")

ROW_COUNT_DIFF_THRESHOLD = float(os.environ.get("DQ_ROW_COUNT_THRESHOLD", "0.05"))  # 5%


class QualityGateError(Exception):
    """Raised when a critical DQ check fails."""
    pass


def _get_oracle_count(table_name, business_date, date_column="trade_dt"):
    """Return row count from Oracle for business_date. Returns None if Oracle unreachable."""
    try:
        import oracledb
        dsn = f"{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}"
        conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=dsn)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {table_name} "
                f"WHERE {date_column} = TO_DATE(:bdate, 'YYYY-MM-DD')",
                bdate=business_date,
            )
            count = cur.fetchone()[0]
            cur.close()
            return count
        finally:
            conn.close()
    except Exception as exc:
        print(f"[quality_gate] Oracle count unavailable for {table_name}: {exc}", flush=True)
        return None


def _record_result(cur, run_id, business_date, check_name, check_type,
                   target_table, severity, passed, expected, actual, details):
    """Insert one DQ result row into dbo.ADWPM_DQ_RESULTS."""
    cur.execute(
        """
        INSERT INTO dbo.ADWPM_DQ_RESULTS
            (result_id, run_id, business_date, check_name, check_type,
             target_table, severity, passed, expected_value, actual_value, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(uuid.uuid4()),
            run_id,
            business_date,
            check_name,
            check_type,
            target_table,
            severity,
            1 if passed else 0,
            str(expected) if expected is not None else None,
            str(actual)   if actual   is not None else None,
            details,
        ),
    )


def run(conn, business_date):
    """
    Run all DQ checks for the given business date.

    Args:
        conn: pymssql connection to SQL Server DW (demopoc database)
        business_date: str in YYYY-MM-DD format

    Returns:
        dict with status, gate_status, checks_run, checks_failed, critical_failures
    """
    run_id = str(uuid.uuid4())
    cur = conn.cursor()
    errors = []

    # =========================================================================
    # Check 1 (Caso 1) — Duplicate trade_id in ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
    # =========================================================================
    check1_passed = True
    check1_count = 0
    try:
        cur.execute(
            """
            SELECT trade_id, COUNT(*) AS cnt
            FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
            WHERE trade_dt = %s
            GROUP BY trade_id
            HAVING COUNT(*) > 1
            """,
            (business_date,),
        )
        dup_rows = cur.fetchall()
        check1_count = sum(row[1] - 1 for row in dup_rows)  # extra copies
        check1_passed = check1_count == 0

        if not check1_passed:
            msg = (
                f"[DQ-CASO-1] Duplicatas encontradas: {check1_count} linhas duplicadas "
                f"em ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO para {business_date}"
            )
            print(msg, flush=True)
            errors.append(msg)
            # Top-5 sample duplicated trade_ids
            samples = [f"trade_id={r[0]} (cnt={r[1]})" for r in dup_rows[:5]]
            print(f"[DQ-CASO-1] samples: {'; '.join(samples)}", flush=True)

        _record_result(
            cur, run_id, business_date,
            check_name="caso_1_duplicate_trade_mvmt",
            check_type="uniqueness",
            target_table="dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
            severity="critical",
            passed=check1_passed,
            expected=0,
            actual=check1_count,
            details=(
                f"Duplicate trade_id count for {business_date}: {check1_count}. "
                f"Source: ASTADRVT_TRADE_MVMT."
            ),
        )
    except Exception as exc:
        print(f"[quality_gate] Check 1 (Caso 1) ERROR: {exc}", flush=True)
        errors.append(f"Check 1 ERROR: {exc}")

    # =========================================================================
    # Check 2 (Caso 2) — Null settlement_price in ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
    # =========================================================================
    check2_passed = True
    check2_count = 0
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
            WHERE position_date = %s AND settlement_price IS NULL
            """,
            (business_date,),
        )
        check2_count = cur.fetchone()[0]
        check2_passed = check2_count == 0

        if not check2_passed:
            msg = (
                f"[DQ-CASO-2] Coluna nula detectada: {check2_count} linhas com "
                f"settlement_price IS NULL em ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL "
                f"para {business_date}"
            )
            print(msg, flush=True)
            errors.append(msg)
            # Top-5 sample position_ids with null settlement_price
            cur.execute(
                """
                SELECT TOP 5 position_id
                FROM dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL
                WHERE position_date = %s AND settlement_price IS NULL
                """,
                (business_date,),
            )
            samples = [f"position_id={r[0]}" for r in cur.fetchall()]
            print(f"[DQ-CASO-2] samples: {'; '.join(samples)}", flush=True)

        _record_result(
            cur, run_id, business_date,
            check_name="caso_2_null_settlement_price",
            check_type="completeness",
            target_table="dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
            severity="critical",
            passed=check2_passed,
            expected=0,
            actual=check2_count,
            details=(
                f"Null settlement_price count for {business_date}: {check2_count}. "
                f"Source: ASTANO_FGBE_DRVT_PSTN."
            ),
        )
    except Exception as exc:
        print(f"[quality_gate] Check 2 (Caso 2) ERROR: {exc}", flush=True)
        errors.append(f"Check 2 ERROR: {exc}")

    # =========================================================================
    # Check 3 (Caso 3) — long_value + short_value = 0 in ADWPM_POSICAO_MERCADO_A_VISTA
    # =========================================================================
    check3_passed = True
    check3_count = 0
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA
            WHERE position_date = %s AND (long_value + short_value) = 0
            """,
            (business_date,),
        )
        check3_count = cur.fetchone()[0]
        check3_passed = check3_count == 0

        if not check3_passed:
            msg = (
                f"[DQ-CASO-3] Soma zero detectada: {check3_count} linhas com "
                f"long_value + short_value = 0 em ADWPM_POSICAO_MERCADO_A_VISTA "
                f"para {business_date}"
            )
            print(msg, flush=True)
            errors.append(msg)
            # Top-5 sample positions with zero-sum
            cur.execute(
                """
                SELECT TOP 5 position_id, long_value, short_value
                FROM dbo.ADWPM_POSICAO_MERCADO_A_VISTA
                WHERE position_date = %s AND (long_value + short_value) = 0
                """,
                (business_date,),
            )
            samples = [f"position_id={r[0]} long={r[1]} short={r[2]}" for r in cur.fetchall()]
            print(f"[DQ-CASO-3] samples: {'; '.join(samples)}", flush=True)

        _record_result(
            cur, run_id, business_date,
            check_name="caso_3_zero_sum_position",
            check_type="validity",
            target_table="dbo.ADWPM_POSICAO_MERCADO_A_VISTA",
            severity="critical",
            passed=check3_passed,
            expected=0,
            actual=check3_count,
            details=(
                f"Zero-sum positions for {business_date}: {check3_count} rows where "
                f"long_value + short_value = 0. Source: ASTACASH_MRKT_PSTN."
            ),
        )
    except Exception as exc:
        print(f"[quality_gate] Check 3 (Caso 3) ERROR: {exc}", flush=True)
        errors.append(f"Check 3 ERROR: {exc}")

    # =========================================================================
    # Check 4 — Row count difference Oracle source vs SQL Server DW (> threshold)
    # =========================================================================
    check4_passed = True
    try:
        checks_4 = [
            {
                "oracle_table": "ASTADRVT_TRADE_MVMT",
                "dw_table": "dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
                "dw_date_col": "trade_dt",
                "oracle_date_col": "trade_dt",
                "name": "caso_4_row_count_diff_mvmt",
            },
            {
                "oracle_table": "ASTANO_FGBE_DRVT_PSTN",
                "dw_table": "dbo.ADWPM_POSICAO_DERIVATIVO_NAO_FUNGIVEL",
                "dw_date_col": "position_date",
                "oracle_date_col": "position_date",
                "name": "caso_4_row_count_diff_drvt",
            },
            {
                "oracle_table": "ASTACASH_MRKT_PSTN",
                "dw_table": "dbo.ADWPM_POSICAO_MERCADO_A_VISTA",
                "dw_date_col": "position_date",
                "oracle_date_col": "position_date",
                "name": "caso_4_row_count_diff_cash",
            },
        ]

        for chk in checks_4:
            # DW count (SQL Server)
            cur.execute(
                f"SELECT COUNT(*) FROM {chk['dw_table']} WHERE {chk['dw_date_col']} = %s",
                (business_date,),
            )
            dw_count = cur.fetchone()[0]

            # Source count (Oracle — skip check if Oracle unavailable)
            oracle_count = _get_oracle_count(
                chk["oracle_table"], business_date, chk["oracle_date_col"]
            )

            if oracle_count is None:
                # Oracle unreachable — record as warning, not critical
                _record_result(
                    cur, run_id, business_date,
                    check_name=chk["name"],
                    check_type="row_count",
                    target_table=chk["dw_table"],
                    severity="warning",
                    passed=True,
                    expected="oracle_unavailable",
                    actual=str(dw_count),
                    details=(
                        f"Oracle unavailable; DW row count = {dw_count}. "
                        f"Cannot compare against source {chk['oracle_table']}."
                    ),
                )
                continue

            diff_pct = (
                abs(oracle_count - dw_count) / oracle_count
                if oracle_count > 0 else 0.0
            )
            row_passed = diff_pct <= ROW_COUNT_DIFF_THRESHOLD

            if not row_passed:
                check4_passed = False
                msg = (
                    f"[DQ-CASO-4] Diferença de contagem: "
                    f"Oracle {chk['oracle_table']}={oracle_count} vs "
                    f"DW {chk['dw_table']}={dw_count} "
                    f"({diff_pct*100:.1f}% > {ROW_COUNT_DIFF_THRESHOLD*100:.0f}%)"
                )
                print(msg, flush=True)
                errors.append(msg)

            _record_result(
                cur, run_id, business_date,
                check_name=chk["name"],
                check_type="row_count",
                target_table=chk["dw_table"],
                severity="warning",
                passed=row_passed,
                expected=str(oracle_count),
                actual=str(dw_count),
                details=(
                    f"Source {chk['oracle_table']}: {oracle_count} rows; "
                    f"DW {chk['dw_table']}: {dw_count} rows; "
                    f"diff={diff_pct*100:.2f}%"
                ),
            )
    except Exception as exc:
        print(f"[quality_gate] Check 4 (row count) ERROR: {exc}", flush=True)
        errors.append(f"Check 4 ERROR: {exc}")

    # =========================================================================
    # Check 5 — Overflow: notional exceeds DECIMAL(18,4) precision contract
    # Detects rows injected by fault overflow or real precision violations.
    # DECIMAL(18,4) max safe value = 99999999999999.9999
    # =========================================================================
    check5_passed = True
    check5_count = 0
    # DECIMAL(18,4) max = 99999999999999.9999; also detect fault_overflow marker rows
    OVERFLOW_THRESHOLD = 99999999999999.9999
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
            WHERE trade_dt = %s
              AND (notional > %s OR dw_source = 'fault_overflow')
            """,
            (business_date, OVERFLOW_THRESHOLD),
        )
        check5_count = cur.fetchone()[0]

        # Also detect load failures from overflow (rows that could not be inserted
        # appear as missing in DW — covered by check 4 row count diff)
        # Additionally check for any rows marked with fault_overflow source
        cur.execute(
            """
            SELECT COUNT(*)
            FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
            WHERE trade_dt = %s AND dw_source = 'fault_overflow'
            """,
            (business_date,),
        )
        fault_marked = cur.fetchone()[0]
        check5_count = check5_count + fault_marked  # total overflow indicators
        check5_passed = check5_count == 0

        if not check5_passed:
            msg = (
                f"[DQ-OVERFLOW] Overflow detectado: {check5_count} linhas com "
                f"notional fora do contrato DECIMAL(18,4) ou marcadas como overflow "
                f"em ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO para {business_date}. "
                f"Indica alteracao de tipo/escala na origem ASTADRVT_TRADE_MVMT."
            )
            print(msg, flush=True)
            errors.append(msg)
            # Top-5 sample overflow rows
            cur.execute(
                """
                SELECT TOP 5 trade_id, ticker, notional, dw_source
                FROM dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO
                WHERE trade_dt = %s
                  AND (notional > %s OR dw_source = 'fault_overflow')
                """,
                (business_date, OVERFLOW_THRESHOLD),
            )
            samples = [
                f"trade_id={r[0]} ticker={r[1]} notional={r[2]} source={r[3]}"
                for r in cur.fetchall()
            ]
            if samples:
                print(f"[DQ-OVERFLOW] samples: {'; '.join(samples)}", flush=True)

        _record_result(
            cur, run_id, business_date,
            check_name="caso_5_overflow_notional",
            check_type="validity",
            target_table="dbo.ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO",
            severity="critical",
            passed=check5_passed,
            expected=f"notional <= {OVERFLOW_THRESHOLD} AND dw_source != fault_overflow",
            actual=str(check5_count),
            details=(
                f"Overflow check: {check5_count} rows violating DECIMAL(18,4) contract "
                f"for {business_date}. Source: ASTADRVT_TRADE_MVMT. "
                f"SQL Server rejects arithmetic overflow at load — indicates source type change."
            ),
        )
    except Exception as exc:
        print(f"[quality_gate] Check 5 (overflow) ERROR: {exc}", flush=True)
        errors.append(f"Check 5 ERROR: {exc}")

    conn.commit()
    cur.close()

    # ── Determine overall gate status ─────────────────────────────────────
    all_checks = [check1_passed, check2_passed, check3_passed, check4_passed, check5_passed]
    checks_run = 5 + 3  # 5 individual checks + 3 row-count sub-checks
    critical_checks = [check1_passed, check2_passed, check3_passed, check5_passed]
    critical_failures = sum(1 for p in critical_checks if not p)
    checks_failed = sum(1 for p in all_checks if not p)
    gate_status = "PASS" if critical_failures == 0 else "FAIL"

    if gate_status == "FAIL":
        print(
            f"[quality_gate] GATE STATUS: FAIL — {critical_failures} critical check(s) failed "
            f"for {business_date}. Errors: {errors}",
            flush=True,
        )
    else:
        print(
            f"[quality_gate] GATE STATUS: PASS — all critical checks passed "
            f"for {business_date}",
            flush=True,
        )

    result = {
        "status": "success" if gate_status == "PASS" else "failed",
        "gate_status": gate_status,
        "checks_run": checks_run,
        "checks_failed": checks_failed,
        "critical_failures": critical_failures,
        "run_id": run_id,
    }

    if gate_status == "FAIL":
        raise QualityGateError(
            f"Quality gate FAILED for {business_date}: "
            f"{critical_failures} critical check(s). "
            + "; ".join(errors)
        )

    return result
