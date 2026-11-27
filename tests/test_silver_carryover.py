"""Testes unitários e de integração para remoção de carryover T1->T2 na Silver (NB-05)."""

from __future__ import annotations

import json
import os
import sys
import types
from pathlib import Path
from typing import Any

import polars as pl
import polars.testing as pl_testing

from tse_analysis.logs.carryover import (
    SILVER_FACT_URNA_EVENT_SCHEMA,
    SILVER_PARSER_VERSION,
    extract_turn1_shard_history,
    strip_turn1_carryover,
    transform_bronze_shard_to_silver,
)
from tse_analysis.logs.event_catalog import load_event_catalog


def _make_bronze_df(
    rows: list[tuple[int, str, str, str, str]],
    *,
    turn: int,
    uf: str = "AC",
    cd_pleito: int | None = None,
    cd_municipio: int = 1007,
    nr_zona: int = 9,
    nr_secao: int = 1,
    source_jez_file: str | None = None,
    urna_serial: str = "02468149",
) -> pl.DataFrame:
    pleito = cd_pleito if cd_pleito is not None else (3220 if turn == 1 else 3221)
    jez = (
        source_jez_file
        or f"o0{pleito}{uf.lower()}{cd_municipio:05d}{nr_zona:04d}{nr_secao:04d}-log.jez"
    )
    n = len(rows)
    return pl.DataFrame(
        {
            "election_year": pl.Series([2026] * n, dtype=pl.Int32),
            "turn": pl.Series([turn] * n, dtype=pl.Int8),
            "uf": pl.Series([uf] * n, dtype=pl.String),
            "cd_pleito": pl.Series([pleito] * n, dtype=pl.Int64),
            "cd_municipio": pl.Series([cd_municipio] * n, dtype=pl.Int32),
            "nr_zona": pl.Series([nr_zona] * n, dtype=pl.Int32),
            "nr_secao": pl.Series([nr_secao] * n, dtype=pl.Int32),
            "source_jez_file": pl.Series([jez] * n, dtype=pl.String),
            "source_log_member": pl.Series(["logd.dat"] * n, dtype=pl.String),
            "file_seq": pl.Series([r[0] for r in rows], dtype=pl.Int64),
            "raw_timestamp": pl.Series([r[1] for r in rows], dtype=pl.String),
            "log_level": pl.Series([r[2] for r in rows], dtype=pl.String),
            "urna_serial": pl.Series([urna_serial] * n, dtype=pl.String),
            "application": pl.Series([r[3] for r in rows], dtype=pl.String),
            "message_text": pl.Series([r[4] for r in rows], dtype=pl.String),
        }
    )


def _turn1_rows(
    start_seq: int = 1, ts_prefix: str = "04/10/2026"
) -> list[tuple[int, str, str, str, str]]:
    return [
        (start_seq, f"{ts_prefix} 07:00:00", "INFO", "LOGD", "Início das operações do logd"),
        (start_seq + 1, f"{ts_prefix} 07:00:01", "INFO", "SCUE", "Iniciando aplicação - 1º turno"),
        (start_seq + 2, f"{ts_prefix} 07:30:00", "INFO", "VOTA", "Emissão da Zerésima"),
        (start_seq + 3, f"{ts_prefix} 08:00:00", "INFO", "VOTA", "Eleitor foi habilitado"),
        (start_seq + 4, f"{ts_prefix} 08:01:00", "INFO", "VOTA", "O voto do eleitor foi computado"),
        (start_seq + 5, f"{ts_prefix} 17:00:00", "INFO", "VOTA", "Encerramento da votação"),
        (
            start_seq + 6,
            f"{ts_prefix} 17:01:00",
            "INFO",
            "VOTA",
            "Gerando arquivo de resultado [log.jez] + [Início]",
        ),
    ]


def _turn2_rows(
    start_seq: int,
    ts_prefix: str = "25/10/2026",
    *,
    include_t2_marker: bool = True,
) -> list[tuple[int, str, str, str, str]]:
    second_msg = (
        "Iniciando aplicação - 2º turno" if include_t2_marker else "Versão da aplicação: 10.23.0.0"
    )
    return [
        (start_seq, f"{ts_prefix} 07:00:00", "INFO", "LOGD", "Início das operações do logd"),
        (start_seq + 1, f"{ts_prefix} 07:00:01", "INFO", "SCUE", second_msg),
        (start_seq + 2, f"{ts_prefix} 07:30:00", "INFO", "VOTA", "Emissão da Zerésima"),
        (start_seq + 3, f"{ts_prefix} 08:05:00", "INFO", "VOTA", "Eleitor foi habilitado"),
        (start_seq + 4, f"{ts_prefix} 08:06:00", "INFO", "VOTA", "O voto do eleitor foi computado"),
        (start_seq + 5, f"{ts_prefix} 17:00:00", "INFO", "VOTA", "Encerramento da votação"),
    ]


def test_turn1_zero_rows_dropped_and_dtypes_standardized() -> None:
    """Turn 1 nunca perde nenhuma linha e padroniza dtypes Silver (Int16/Int8/String)."""
    rows = _turn1_rows()
    # Inclui uma linha com regressão temporal e uma com milissegundos
    rows.append(
        (
            8,
            "04/10/2026 16:59:59,250",
            "INFO",
            "VOTA",
            "Gerando arquivo de resultado [log.jez] + [Término]",
        )
    )
    df_t1 = _make_bronze_df(rows, turn=1)

    silver_df, stats = transform_bronze_shard_to_silver(df_t1, election_year=2026, turn=1, uf="AC")
    assert stats.raw_rows == 8
    assert stats.carryover_rows_dropped == 0
    assert stats.silver_rows_kept == 8
    assert silver_df.schema == SILVER_FACT_URNA_EVENT_SCHEMA
    assert silver_df["file_seq"].to_list() == list(range(1, 9))
    assert silver_df["has_timestamp_regression"].to_list()[-1] is True
    assert silver_df["is_clock_drift"].to_list() == [False] * 8
    assert silver_df["parser_version"].unique().to_list() == [SILVER_PARSER_VERSION]


def test_turn2_reused_urn_normal_timestamps_strips_exact_turn1_prefix() -> None:
    """Urna reutilizada no 2º turno com marcador remove 100% do prefixo T1 e preserva T2."""
    t1 = _turn1_rows(start_seq=1)
    t2 = _turn2_rows(start_seq=8, include_t2_marker=True)
    df_t2 = _make_bronze_df(t1 + t2, turn=2)

    filtered, stats = strip_turn1_carryover(df_t2, election_year=2026, turn=2)
    assert stats.raw_rows == 13
    assert stats.carryover_rows_dropped == 7
    assert stats.silver_rows_kept == 6
    assert stats.jez_resolution_counts["lifecycle_marker"] == 1
    assert filtered["file_seq"].to_list() == list(range(8, 14))
    assert filtered["is_clock_drift"].to_list() == [False] * 6


def test_turn2_reused_urn_bios_clock_reset_1970_and_2026_jan() -> None:
    """Urna com reset de BIOS (1970/jan-2026) remove T1 e retém T2 com is_clock_drift=True."""
    t1 = _turn1_rows(start_seq=1, ts_prefix="01/01/1970")
    t2 = _turn2_rows(start_seq=8, ts_prefix="01/01/1970", include_t2_marker=True)
    # Adiciona evento com outro padrão de reset (01/01/2026)
    t2.append(
        (
            14,
            "01/01/2026 00:01:10",
            "INFO",
            "VOTA",
            "Gerando arquivo de resultado [log.jez] + [Início]",
        )
    )

    df_t2 = _make_bronze_df(t1 + t2, turn=2)
    silver_df, stats = transform_bronze_shard_to_silver(df_t2, election_year=2026, turn=2, uf="SP")

    assert stats.raw_rows == 14
    assert stats.carryover_rows_dropped == 7
    assert stats.silver_rows_kept == 7
    assert stats.clock_drift_rows_retained == 7
    assert stats.jez_resolution_counts["lifecycle_marker"] == 1
    assert silver_df["file_seq"].to_list() == list(range(8, 15))
    assert all(silver_df["is_clock_drift"].to_list())


def test_turn2_seq_overlap_without_text_marker_and_wiped_flash_guard() -> None:
    """Sem marcador textual de 2º turno, usa t1_max_file_seq; protege flash formatada."""
    df_t1 = _make_bronze_df(_turn1_rows(start_seq=1), turn=1, urna_serial="URNA_OVERLAP")
    hist = extract_turn1_shard_history(df_t1)

    # Caso A: mesma urna com prefixo T1 intacto e T2 sem string explícita de '2º turno'
    t1_prefix = _turn1_rows(start_seq=1)
    t2_tail = _turn2_rows(start_seq=8, ts_prefix="01/01/1970", include_t2_marker=False)
    df_t2_overlap = _make_bronze_df(t1_prefix + t2_tail, turn=2, urna_serial="URNA_OVERLAP")

    filtered_a, stats_a = strip_turn1_carryover(
        df_t2_overlap, election_year=2026, turn=2, turn1_history=hist
    )
    assert stats_a.carryover_rows_dropped == 7
    assert stats_a.silver_rows_kept == 6
    assert stats_a.jez_resolution_counts["turn1_urna_seq_overlap"] == 1
    assert filtered_a["file_seq"].to_list() == list(range(8, 14))

    # Caso B: mesma urna física teve flash formatada antes do T2 (seq=1 em 25/10)
    t2_wiped = _turn2_rows(start_seq=1, ts_prefix="25/10/2026", include_t2_marker=False)
    df_t2_wiped = _make_bronze_df(t2_wiped, turn=2, urna_serial="URNA_OVERLAP")

    filtered_b, stats_b = strip_turn1_carryover(
        df_t2_wiped, election_year=2026, turn=2, turn1_history=hist
    )
    assert stats_b.carryover_rows_dropped == 0
    assert stats_b.silver_rows_kept == 6
    assert filtered_b["file_seq"].to_list() == list(range(1, 7))


def test_turn2_fresh_contingency_urn_keeps_100_percent() -> None:
    """Urna nova/contingência no 2º turno (sem histórico T1) mantém 100% das linhas."""
    t1_other = _make_bronze_df(_turn1_rows(start_seq=1), turn=1, urna_serial="URNA_T1_ONLY")
    hist = extract_turn1_shard_history(t1_other)

    t2_fresh = _turn2_rows(start_seq=1, ts_prefix="25/10/2026", include_t2_marker=True)
    df_fresh = _make_bronze_df(t2_fresh, turn=2, urna_serial="URNA_CONTINGENCIA_NOVA")

    filtered, stats = strip_turn1_carryover(
        df_fresh, election_year=2026, turn=2, turn1_history=hist
    )
    assert stats.carryover_rows_dropped == 0
    assert stats.silver_rows_kept == 6
    assert stats.jez_resolution_counts["fresh_urna_no_carryover"] == 1


def test_turn2_date_fallback_and_unresolved_abstention() -> None:
    """Fallback de data atua sem marcador/overlap; relógio 1970 sem marcador abstém."""
    t1_no_marker = [
        (1, "04/10/2026 08:00:00", "INFO", "VOTA", "Eleitor foi habilitado"),
        (2, "04/10/2026 17:00:00", "INFO", "VOTA", "Encerramento da votação"),
    ]
    t2_no_marker = [
        (3, "25/10/2026 07:00:00", "INFO", "LOGD", "Início das operações do logd"),
        (4, "25/10/2026 08:00:00", "INFO", "VOTA", "Eleitor foi habilitado"),
    ]
    df_fb = _make_bronze_df(t1_no_marker + t2_no_marker, turn=2, urna_serial="URNA_NO_HIST")
    filtered_fb, stats_fb = strip_turn1_carryover(
        df_fb, election_year=2026, turn=2, turn1_history=None
    )
    assert stats_fb.carryover_rows_dropped == 2
    assert stats_fb.silver_rows_kept == 2
    assert stats_fb.jez_resolution_counts["date_fallback"] == 1
    assert filtered_fb["file_seq"].to_list() == [3, 4]

    # Se tudo estiver em 1970 e sem histórico T1 nem marcador T2, retém como unresolved
    t_1970_ambiguous = [
        (1, "01/01/1970 00:01:00", "INFO", "SCUE", "Iniciando aplicação - 1º turno"),
        (2, "01/01/1970 00:02:00", "INFO", "VOTA", "Eleitor foi habilitado"),
    ]
    df_unres = _make_bronze_df(t_1970_ambiguous, turn=2, urna_serial="URNA_NO_HIST")
    filtered_unres, stats_unres = strip_turn1_carryover(
        df_unres, election_year=2026, turn=2, turn1_history=None
    )
    assert stats_unres.carryover_rows_dropped == 0
    assert stats_unres.silver_rows_kept == 2
    assert stats_unres.jez_resolution_counts["unresolved"] == 1
    assert all(filtered_unres["is_clock_drift"].to_list())


def test_turn2_reassigned_urn_section_a_to_b_no_session_double_count() -> None:
    """Urna realocada da Seção A (T1) para Seção B (T2) não duplica sessões no NB-06."""
    df_t1_sec_a = _make_bronze_df(
        _turn1_rows(start_seq=1),
        turn=1,
        uf="AC",
        cd_municipio=1007,
        nr_zona=9,
        nr_secao=10,
        urna_serial="URNA_REALLOC",
    )
    hist = extract_turn1_shard_history(df_t1_sec_a)

    # No 2º turno a urna foi para Seção B (nr_secao=99, outra UF), com 1 voto T1 + 2 votos T2
    t2_rows = _turn2_rows(start_seq=8, include_t2_marker=True)
    t2_rows.insert(
        4, (11, "25/10/2026 08:05:30", "INFO", "VOTA", "O voto do eleitor foi computado")
    )
    df_t2_sec_b = _make_bronze_df(
        _turn1_rows(start_seq=1) + t2_rows,
        turn=2,
        uf="RO",
        cd_municipio=2001,
        nr_zona=5,
        nr_secao=99,
        urna_serial="URNA_REALLOC",
    )

    silver_t2, stats = transform_bronze_shard_to_silver(
        df_t2_sec_b,
        election_year=2026,
        turn=2,
        uf="RO",
        turn1_history=hist,
    )
    assert stats.carryover_rows_dropped == 7
    assert silver_t2["section_num"].unique().to_list() == [99]
    # Apenas os 2 votos do T2 permanecem (não 3 com o carryover do T1)
    assert silver_t2.filter(pl.col("event_code") == "VOTER_VOTE_COMPUTED").height == 2


def test_carryover_filter_idempotency() -> None:
    """Executar strip_turn1_carryover duas vezes produz saída idêntica."""
    df_t1 = _make_bronze_df(_turn1_rows(start_seq=1), turn=1, urna_serial="URNA_IDEM")
    hist = extract_turn1_shard_history(df_t1)

    df_t2 = _make_bronze_df(
        _turn1_rows(start_seq=1) + _turn2_rows(start_seq=8), turn=2, urna_serial="URNA_IDEM"
    )

    pass1_df, stats1 = strip_turn1_carryover(df_t2, election_year=2026, turn=2, turn1_history=hist)
    pass2_df, stats2 = strip_turn1_carryover(
        pass1_df, election_year=2026, turn=2, turn1_history=hist
    )

    assert stats1.carryover_rows_dropped == 7
    assert stats2.carryover_rows_dropped == 0
    pl_testing.assert_frame_equal(pass1_df, pass2_df)

    # Também valida idempotência quando executado sobre a saída já materializada na Silver
    silver_df, _ = transform_bronze_shard_to_silver(
        df_t2, election_year=2026, turn=2, uf="AC", turn1_history=hist
    )
    silver_pass2, stats_silver2 = strip_turn1_carryover(
        silver_df, election_year=2026, turn=2, turn1_history=hist
    )
    assert stats_silver2.carryover_rows_dropped == 0
    pl_testing.assert_frame_equal(silver_df, silver_pass2)


def test_nb05_parity_and_end_to_end_execution(repo_root: Path, tmp_path: Path) -> None:
    """Valida paridade do NB-05 com event_catalog.yml e executa ponta a ponta em tmp_path."""
    nb_path = (
        repo_root / "notebooks" / "05_silver_fact_urna_event" / "05_silver_fact_urna_event.ipynb"
    )
    nb_data = json.loads(nb_path.read_text(encoding="utf-8"))

    # Preparar dataset Bronze sintético em tmp_path
    dataset_dir = tmp_path / "dataset"
    staging_dir = tmp_path / "staging"
    t1_dir = dataset_dir / "fact_urna_logs" / "election_year=2026" / "turn=1" / "uf=AC"
    t2_dir = dataset_dir / "fact_urna_logs" / "election_year=2026" / "turn=2" / "uf=AC"
    t1_dir.mkdir(parents=True, exist_ok=True)
    t2_dir.mkdir(parents=True, exist_ok=True)

    df_t1 = _make_bronze_df(_turn1_rows(start_seq=1), turn=1, uf="AC", urna_serial="URNA_NB05")
    df_t2 = _make_bronze_df(
        _turn1_rows(start_seq=1)
        + _turn2_rows(start_seq=8, ts_prefix="01/01/1970", include_t2_marker=True),
        turn=2,
        uf="AC",
        urna_serial="URNA_NB05",
    )
    df_t1.write_parquet(t1_dir / "part-0.parquet", compression="zstd")
    df_t2.write_parquet(t2_dir / "part-0.parquet", compression="zstd")

    old_env = {
        k: os.environ.get(k)
        for k in ("DATASET_PATH", "STAGING_PATH", "TSE_TARGET_UFS", "TSE_SKIP_KAGGLE_RELEASE")
    }
    os.environ["DATASET_PATH"] = str(dataset_dir)
    os.environ["STAGING_PATH"] = str(staging_dir)
    os.environ["TSE_TARGET_UFS"] = "AC"
    os.environ["TSE_SKIP_KAGGLE_RELEASE"] = "1"
    try:
        mod = types.ModuleType("nb05_test_runner")
        ns: dict[str, Any] = mod.__dict__
        sys.modules["nb05_test_runner"] = mod
        for idx, cell in enumerate(nb_data["cells"]):
            if cell["cell_type"] == "code":
                exec(compile("".join(cell["source"]), f"{nb_path}#cell{idx}", "exec"), ns)
    finally:
        sys.modules.pop("nb05_test_runner", None)
        for k, v in old_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    # Paridade de catálogo
    repo_cat = load_event_catalog(repo_root / "configs" / "event_catalog.yml")
    nb_cat = ns["CATALOG"]
    assert nb_cat.turn_boundary_patterns.turn1_init == repo_cat.turn_boundary_patterns.turn1_init
    assert nb_cat.turn_boundary_patterns.turn2_init == repo_cat.turn_boundary_patterns.turn2_init
    assert nb_cat.turn_boundary_patterns.boot_header == repo_cat.turn_boundary_patterns.boot_header
    assert nb_cat.turn_boundary_patterns.turn1_close == repo_cat.turn_boundary_patterns.turn1_close

    # Conservação de linhas e artefatos Silver gerados
    assert ns["bronze_total"] == 20
    assert ns["carryover_dropped_total"] == 7
    assert ns["silver_total"] == 13
    assert ns["clock_drift_retained_total"] == 6

    silver_out = dataset_dir / "silver" / "fact_urna_event"
    manifest = json.loads((silver_out / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["bronze_total_rows"] == 20
    assert manifest["carryover_dropped_total"] == 7
    assert manifest["silver_total_rows"] == 13
    assert manifest["clock_drift_rows_retained"] == 6
    assert list(silver_out.rglob("*.tmp")) == []

    # Apenas arquivos Parquet de eventos existem em silver/fact_urna_event (sem auditoria)
    silver_pqs = sorted(silver_out.rglob("*.parquet"))
    assert len(silver_pqs) == 2
    df_silver_t2 = pl.read_parquet(
        silver_out / "election_year=2026" / "turn=2" / "uf=AC" / "part-0.parquet"
    )
    assert df_silver_t2.schema == SILVER_FACT_URNA_EVENT_SCHEMA
    assert df_silver_t2["file_seq"].to_list() == list(range(8, 14))
