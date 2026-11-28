"""Testes unitários da camada Gold (NB-08), Release Pipeline (NB-09) e higiene pt-BR."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import polars as pl

REPO_ROOT = Path(__file__).resolve().parent.parent
NB08_PATH = REPO_ROOT / "notebooks" / "08_gold_analytics" / "08_gold_analytics.ipynb"
NB09_PATH = REPO_ROOT / "notebooks" / "09_release_pipeline" / "09_release_pipeline.ipynb"


def _load_nb08_functions(dataset_dir: Path, staging_dir: Path) -> dict[str, object]:
    """Executa apenas as células de setup e funções puras do NB-08 em namespace isolado."""
    nb = json.loads(NB08_PATH.read_text(encoding="utf-8"))
    code_cells = [c for c in nb["cells"] if c.get("cell_type") == "code"]
    ns: dict[str, object] = {"__name__": "test_nb08"}

    old_ds = os.environ.get("DATASET_PATH")
    old_st = os.environ.get("STAGING_PATH")
    os.environ["DATASET_PATH"] = str(dataset_dir)
    os.environ["STAGING_PATH"] = str(staging_dir)
    try:
        for cell in code_cells[:2]:
            src = "".join(cell["source"])
            exec(compile(src, "08_gold_analytics.ipynb", "exec"), ns)
    finally:
        if old_ds is None:
            os.environ.pop("DATASET_PATH", None)
        else:
            os.environ["DATASET_PATH"] = old_ds
        if old_st is None:
            os.environ.pop("STAGING_PATH", None)
        else:
            os.environ["STAGING_PATH"] = old_st
    return ns


def test_nb08_streaming_gold_and_clock_drift_guard() -> None:
    """Valida streaming das 5 tabelas Gold, guarda de clock drift e prevenção de overflow Int32."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        ds_dir = root / "data"
        st_dir = root / "staging"
        ev_dir = ds_dir / "silver" / "fact_urna_event" / "election_year=2026" / "turn=1" / "uf=AC"
        ev_dir.mkdir(parents=True)

        # Shard 0: seção normal (sem clock drift)
        shard0 = pl.DataFrame({
            "election_year": pl.Series([2026, 2026, 2026, 2026, 2026], dtype=pl.Int16),
            "turn": pl.Series([1, 1, 1, 1, 1], dtype=pl.Int8),
            "uf": pl.Series(["AC"] * 5, dtype=pl.String),
            "municipio_code": pl.Series([1015] * 5, dtype=pl.Int32),
            "zone_num": pl.Series([1] * 5, dtype=pl.Int16),
            "section_num": pl.Series([10] * 5, dtype=pl.Int32),
            "event_code": pl.Series(
                [
                    "VOTER_ENABLED",
                    "BIOMETRIC_READ_ATTEMPT",
                    "BIOMETRIC_RECOGNIZED",
                    "MANUAL_ENABLE_PATH",
                    "INVALID_KEY_PRESSED",
                ],
                dtype=pl.String,
            ),
            "biometric_attempt_num": pl.Series([None, 1, None, None, None], dtype=pl.Int8),
            "classification_status": pl.Series(["CLASSIFIED"] * 5, dtype=pl.String),
            "is_clock_drift": pl.Series([False] * 5, dtype=pl.Boolean),
            "has_timestamp_regression": pl.Series([False] * 5, dtype=pl.Boolean),
        })
        shard0.write_parquet(ev_dir / "part-0.parquet")

        # Shard 1: seção com clock drift de BIOS (1970) e evento UNKNOWN
        shard1 = pl.DataFrame({
            "election_year": pl.Series([2026, 2026, 2026], dtype=pl.Int16),
            "turn": pl.Series([1, 1, 1], dtype=pl.Int8),
            "uf": pl.Series(["AC"] * 3, dtype=pl.String),
            "municipio_code": pl.Series([1015] * 3, dtype=pl.Int32),
            "zone_num": pl.Series([1] * 3, dtype=pl.Int16),
            "section_num": pl.Series([20] * 3, dtype=pl.Int32),
            "event_code": pl.Series(
                ["BIOMETRIC_READ_ATTEMPT", "BIOMETRIC_NOT_RECOGNIZED", "UNKNOWN_EVENT"],
                dtype=pl.String,
            ),
            "biometric_attempt_num": pl.Series([2, None, None], dtype=pl.Int8),
            "classification_status": pl.Series(
                ["CLASSIFIED", "CLASSIFIED", "UNKNOWN"], dtype=pl.String
            ),
            "is_clock_drift": pl.Series([True, True, False], dtype=pl.Boolean),
            "has_timestamp_regression": pl.Series([False, True, False], dtype=pl.Boolean),
        })
        shard1.write_parquet(ev_dir / "part-1.parquet")

        ns = _load_nb08_functions(ds_dir, st_dir)
        stream_res = ns["aggregate_urna_events_streaming"](
            [ev_dir / "part-0.parquet", ev_dir / "part-1.parquet"]
        )
        assert stream_res["total_input_events"] == 8
        assert stream_res["total_bio_rows"] == 2

        gold_dist = ns["build_gold_event_distribution"](stream_res["event_dist_raw"])
        assert gold_dist["event_count"].sum() == 8
        assert gold_dist["clock_drift_count"].sum() == 2

        gold_bio = ns["build_gold_biometric_attempts"](stream_res["bio_raw"])
        assert gold_bio["attempt_count"].sum() == 2
        assert gold_bio.sort("biometric_attempt_num")["cumulative_share"].to_list() == [50.0, 100.0]

        # Sessões sintéticas — testa também regressão de soma Int32 > 2^31
        t_start = datetime(2026, 10, 4, 8, 0)
        t_end = datetime(2026, 10, 4, 17, 0)
        df_sessions_overflow = pl.DataFrame({
            "election_year": pl.Series([2026, 2026], dtype=pl.Int16),
            "turn": pl.Series([1, 1], dtype=pl.Int8),
            "uf": pl.Series(["SP", "SP"], dtype=pl.String),
            "municipio_code": pl.Series([71072, 71072], dtype=pl.Int32),
            "zone_num": pl.Series([1, 1], dtype=pl.Int16),
            "section_num": pl.Series([1, 2], dtype=pl.Int32),
            "urna_serial": pl.Series(["U1", "U2"], dtype=pl.String),
            "first_event_ts": pl.Series([t_start, t_start]),
            "last_event_ts": pl.Series([t_end, t_end]),
            "final_state": pl.Series(["CLOSED", "CLOSED"], dtype=pl.String),
            "is_complete": pl.Series([True, True], dtype=pl.Boolean),
            "has_contingency": pl.Series([False, True], dtype=pl.Boolean),
            "voter_cycles": pl.Series([1_500_000_000, 1_500_000_000], dtype=pl.Int32),
            "event_count": pl.Series([1_500_000_000, 1_500_000_000], dtype=pl.Int32),
            "state_machine_version": pl.Series(["1.0.0", "1.0.0"], dtype=pl.String),
        })
        gold_comp_big = ns["build_gold_session_completeness"](df_sessions_overflow)
        assert gold_comp_big["total_events"][0] == 3_000_000_000
        assert gold_comp_big["total_voter_cycles"][0] == 3_000_000_000
        assert gold_comp_big["contingency_session_count"][0] == 1

        # Sessões AC para validar gold_section_turn_summary e gold_municipality_turn_summary
        t_drift = datetime(1970, 1, 1, 0, 0, 0)
        df_sessions_ac = pl.DataFrame({
            "election_year": pl.Series([2026, 2026], dtype=pl.Int16),
            "turn": pl.Series([1, 1], dtype=pl.Int8),
            "uf": pl.Series(["AC", "AC"], dtype=pl.String),
            "municipio_code": pl.Series([1015, 1015], dtype=pl.Int32),
            "zone_num": pl.Series([1, 1], dtype=pl.Int16),
            "section_num": pl.Series([10, 20], dtype=pl.Int32),
            "urna_serial": pl.Series(["001", "002"], dtype=pl.String),
            "first_event_ts": pl.Series([t_start, t_drift]),
            "last_event_ts": pl.Series([t_end, t_end]),
            "final_state": pl.Series(["CLOSED", "INCOMPLETE"], dtype=pl.String),
            "is_complete": pl.Series([True, False], dtype=pl.Boolean),
            "has_contingency": pl.Series([False, True], dtype=pl.Boolean),
            "voter_cycles": pl.Series([200, 100], dtype=pl.Int32),
            "event_count": pl.Series([5, 3], dtype=pl.Int32),
            "state_machine_version": pl.Series(["1.0.0", "1.0.0"], dtype=pl.String),
        })

        df_bu = pl.DataFrame({
            "turn": pl.Series([1, 1], dtype=pl.Int8),
            "uf": pl.Series(["AC", "AC"], dtype=pl.String),
            "municipio_code": pl.Series([1015, 1015], dtype=pl.Int32),
            "zone_num": pl.Series([1, 1], dtype=pl.Int16),
            "section_num": pl.Series([10, 20], dtype=pl.Int32),
            "bu_municipio_name": pl.Series(["RIO BRANCO", "RIO BRANCO"], dtype=pl.String),
            "bu_local_votacao_num": pl.Series([1001, 1002], dtype=pl.Int32),
            "qt_aptos": pl.Series([250, 150], dtype=pl.Int64),
            "qt_comparecimento": pl.Series([200, 100], dtype=pl.Int64),
            "qt_abstencoes": pl.Series([50, 50], dtype=pl.Int64),
            "qt_elei_biom_sem_habilitacao": pl.Series([5, 2], dtype=pl.Int64),
        })
        df_loc = ns["load_bronze_local_votacao"]([])
        df_perfil = ns["load_bronze_eleitorado_perfil"]([])
        df_corresp = ns["load_bronze_correspondencia"]([])
        df_modelo = ns["load_bronze_urna_model"]([])

        gold_sec = ns["build_gold_section_turn_summary"](
            df_sessions_ac,
            stream_res["section_events"],
            df_bu,
            df_loc,
            df_perfil,
            df_corresp,
            df_modelo,
        )
        assert gold_sec.height == 2
        row_ok = gold_sec.filter(pl.col("section_num") == 10)
        row_drift = gold_sec.filter(pl.col("section_num") == 20)
        assert row_ok["observed_duration_sec"][0] == 9 * 3600
        assert row_ok["turnout_rate_pct"][0] == 80.0
        # Guarda crítica: seção com clock drift de 1970 MUST ter observed_duration_sec == None
        assert row_drift["has_clock_drift"][0] is True
        assert row_drift["observed_duration_sec"][0] is None

        gold_mun = ns["build_gold_municipality_turn_summary"](gold_sec)
        assert gold_mun.height == 1
        assert gold_mun["section_count"][0] == 2
        assert gold_mun["closed_section_count"][0] == 1
        assert gold_mun["clock_drift_section_count"][0] == 1
        assert gold_mun["total_events"][0] == 8
        assert gold_mun["qt_comparecimento"][0] == 300
        assert gold_mun["avg_observed_duration_sec"][0] == float(9 * 3600)

        # Valida escrita staged e leitura DuckDB 2.0
        out_p = ns["write_gold_parquet_staged"](
            gold_sec, ds_dir / "gold" / "gold_section_turn_summary.parquet"
        )
        con = duckdb.connect(":memory:")
        cnt = con.execute(f"SELECT count(*) FROM read_parquet('{out_p}')").fetchone()[0]
        con.close()
        assert cnt == 2


def test_nb09_registry_covers_all_28_tables_and_skip_guard() -> None:
    """Garante que o NB-09 registra todas as 28 tabelas e respeita TSE_SKIP_KAGGLE_RELEASE."""
    nb = json.loads(NB09_PATH.read_text(encoding="utf-8"))
    code = "\n".join("".join(c["source"]) for c in nb["cells"] if c.get("cell_type") == "code")
    for required_table in (
        "gold_event_distribution",
        "gold_session_completeness",
        "gold_biometric_attempts",
        "gold_section_turn_summary",
        "gold_municipality_turn_summary",
        "fact_urna_event",
        "fact_voting_session",
        "dim_election",
        "dim_cargo",
        "dim_partido",
        "fact_urna_logs",
    ):
        assert f'"{required_table}"' in code, f"Tabela ausente em NB-09: {required_table}"
    assert "TSE_SKIP_KAGGLE_RELEASE" in code
    assert "is_clock_drift" in code
    assert '"event_catalog": "1.1.0"' in code


def test_all_9_notebooks_in_brazilian_portuguese() -> None:
    """Verifica ausência de frases residuais em inglês nos 9 notebooks faseados."""
    forbidden_english_markers = (
        "Headless/CLI execution detected",
        "userdata unavailable in CLI mode",
        "relying on environment variables",
        "ORPHAN KEY REPORT",
        "folder does not exist",
        "no parquet files found",
        "Manifest written to:",
        "READY FOR RELEASE PUBLICATION",
        "Mismatched event count",
        "Reconstruct voting sessions from urna events",
    )
    for nb_path in sorted((REPO_ROOT / "notebooks").glob("*/*.ipynb")):
        raw = nb_path.read_text(encoding="utf-8")
        for marker in forbidden_english_markers:
            assert marker not in raw, f"Texto em inglês residual em {nb_path.name}: {marker!r}"
