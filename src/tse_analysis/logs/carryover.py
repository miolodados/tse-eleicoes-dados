"""Remoção determinística de carryover do 1º turno no 2º turno (clock-drift-safe) para Silver."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import polars as pl

from tse_analysis.logs.event_catalog import EventCatalog, _get_default_event_catalog

SILVER_PARSER_VERSION: str = "1.1.0"

RESOLUTION_METHODS: tuple[str, ...] = (
    "lifecycle_marker",
    "turn1_urna_seq_overlap",
    "date_fallback",
    "fresh_urna_no_carryover",
    "unresolved",
)

SILVER_FACT_URNA_EVENT_SCHEMA = pl.Schema(
    {
        "election_year": pl.Int16,
        "turn": pl.Int8,
        "uf": pl.String,
        "municipio_code": pl.Int32,
        "zone_num": pl.Int16,
        "section_num": pl.Int32,
        "urna_serial": pl.String,
        "file_seq": pl.Int64,
        "raw_timestamp": pl.Datetime("us"),
        "is_clock_drift": pl.Boolean,
        "has_timestamp_regression": pl.Boolean,
        "application": pl.String,
        "log_level": pl.String,
        "event_code": pl.String,
        "biometric_attempt_num": pl.Int8,
        "classification_status": pl.String,
        "parser_version": pl.String,
    }
)

TURN1_HISTORY_SCHEMA = pl.Schema(
    {
        "urna_serial": pl.String,
        "t1_max_file_seq": pl.Int64,
        "t1_first_ts_raw": pl.String,
        "t1_max_seq_ts_raw": pl.String,
    }
)

_SESSION_ACTIVITY_REGEX: str = (
    r"(?i)(?:urna\s+pronta\s+para\s+receber\s+votos"
    r"|in[ií]cio\s+da\s+vota[cç][aã]o"
    r"|emiss[aã]o\s+d[ae]\s+zer[eé]sima"
    r"|eleitor\s+(?:foi\s+)?habilitado"
    r"|voto\s+do\s+eleitor"
    r"|modo\s+de\s+conting[eê]ncia)"
)


@dataclass(frozen=True, slots=True)
class CarryoverFilterStats:
    """Estatísticas de auditoria da remoção de carryover do 1º turno em um lote/partição."""

    raw_rows: int
    carryover_rows_dropped: int
    silver_rows_kept: int
    clock_drift_rows_retained: int
    jez_files_total: int
    jez_resolution_counts: dict[str, int]


def _empty_resolution_counts() -> dict[str, int]:
    return {method: 0 for method in RESOLUTION_METHODS}


def default_inter_turn_cutoff(election_year: int) -> datetime:
    """Retorna a data de corte padrão entre 1º e 2º turno para fallback temporal."""
    return datetime(election_year, 10, 10, 0, 0, 0)


def election_clock_window(election_year: int) -> tuple[datetime, datetime]:
    """Janela operacional válida para detecção de clock drift da BIOS da urna."""
    return (
        datetime(election_year, 8, 1, 0, 0, 0),
        datetime(election_year, 12, 31, 23, 59, 59, 999999),
    )


def parse_bronze_timestamps_expr(
    col_name: str = "raw_timestamp", *, dtype: pl.DataType | None = None
) -> pl.Expr:
    """Expressão Polars vetorizada para converter raw_timestamp em Datetime('us')."""
    if dtype is not None and isinstance(dtype, pl.Datetime):
        return pl.col(col_name).cast(pl.Datetime("us"))

    cleaned = pl.col(col_name).cast(pl.String).str.strip_chars().str.replace(",", ".", literal=True)
    return pl.coalesce(
        [
            cleaned.str.to_datetime("%d/%m/%Y %H:%M:%S", time_unit="us", strict=False),
            cleaned.str.to_datetime("%d/%m/%Y %H:%M:%S%.f", time_unit="us", strict=False),
            cleaned.str.to_datetime("%Y-%m-%d %H:%M:%S", time_unit="us", strict=False),
            cleaned.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", time_unit="us", strict=False),
        ]
    )


def compute_clock_drift_expr(ts_expr: pl.Expr, election_year: int) -> pl.Expr:
    """Flag booleana True quando o timestamp válido está fora da janela eleitoral esperada."""
    win_start, win_end = election_clock_window(election_year)
    return (
        ts_expr.is_not_null() & ((ts_expr < pl.lit(win_start)) | (ts_expr > pl.lit(win_end)))
    ).fill_null(False)


def _jez_key_expr(columns: Iterable[str]) -> pl.Expr:
    cols = set(columns)
    if "source_jez_file" in cols:
        return pl.col("source_jez_file").cast(pl.String).fill_null("UNKNOWN_JEZ")

    mun_col = "cd_municipio" if "cd_municipio" in cols else "municipio_code"
    zone_col = "nr_zona" if "nr_zona" in cols else "zone_num"
    sec_col = "nr_secao" if "nr_secao" in cols else "section_num"
    parts = [
        pl.col("uf").cast(pl.String).fill_null("") if "uf" in cols else pl.lit(""),
        pl.col(mun_col).cast(pl.String).fill_null("") if mun_col in cols else pl.lit(""),
        pl.col(zone_col).cast(pl.String).fill_null("") if zone_col in cols else pl.lit(""),
        pl.col(sec_col).cast(pl.String).fill_null("") if sec_col in cols else pl.lit(""),
        pl.col("urna_serial").cast(pl.String).fill_null("")
        if "urna_serial" in cols
        else pl.lit(""),
    ]
    return pl.concat_str(parts, separator=":")


def extract_turn1_shard_history(df_turn1: pl.DataFrame) -> pl.DataFrame:
    """Extrai resumo compacto (urna_serial, t1_max_file_seq, timestamps) de um shard de 1º turno."""
    if df_turn1.is_empty():
        return pl.DataFrame(schema=TURN1_HISTORY_SCHEMA)

    valid = df_turn1.filter(
        pl.col("urna_serial").is_not_null()
        & (pl.col("urna_serial").cast(pl.String).str.strip_chars() != "")
        & (pl.col("urna_serial").cast(pl.String).str.strip_chars() != "UNKNOWN")
    )
    if valid.is_empty():
        return pl.DataFrame(schema=TURN1_HISTORY_SCHEMA)

    return (
        valid.with_columns(
            pl.col("urna_serial").cast(pl.String).str.strip_chars().alias("urna_serial"),
            pl.col("file_seq").cast(pl.Int64).alias("file_seq"),
            pl.col("raw_timestamp").cast(pl.String).alias("_raw_ts_str"),
        )
        .group_by("urna_serial")
        .agg(
            pl.col("file_seq").max().cast(pl.Int64).alias("t1_max_file_seq"),
            pl.col("_raw_ts_str").sort_by("file_seq").first().alias("t1_first_ts_raw"),
            pl.col("_raw_ts_str").sort_by("file_seq").last().alias("t1_max_seq_ts_raw"),
        )
        .select(list(TURN1_HISTORY_SCHEMA.names()))
    )


def merge_turn1_history(
    existing: pl.DataFrame | None,
    new_chunk: pl.DataFrame | None,
) -> pl.DataFrame:
    """Combina dois resumos de histórico de 1º turno mantendo o maior t1_max_file_seq por urna."""
    frames = [f for f in (existing, new_chunk) if f is not None and not f.is_empty()]
    if not frames:
        return pl.DataFrame(schema=TURN1_HISTORY_SCHEMA)

    normalized: list[pl.DataFrame] = []
    for frame in frames:
        exprs: list[pl.Expr] = [
            pl.col("urna_serial").cast(pl.String),
            pl.col("t1_max_file_seq").cast(pl.Int64),
        ]
        if "t1_first_ts_raw" in frame.columns:
            exprs.append(pl.col("t1_first_ts_raw").cast(pl.String))
        else:
            exprs.append(pl.lit(None, dtype=pl.String).alias("t1_first_ts_raw"))
        if "t1_max_seq_ts_raw" in frame.columns:
            exprs.append(pl.col("t1_max_seq_ts_raw").cast(pl.String))
        else:
            exprs.append(pl.lit(None, dtype=pl.String).alias("t1_max_seq_ts_raw"))
        normalized.append(frame.select(exprs))

    combined = pl.concat(normalized, how="vertical")
    return (
        combined.sort("t1_max_file_seq", descending=True)
        .unique(subset=["urna_serial"], keep="first")
        .select(list(TURN1_HISTORY_SCHEMA.names()))
    )


def scan_turn1_urna_history(parquet_files: Iterable[str | Path]) -> pl.DataFrame:
    """Constrói histórico compacto de 1º turno em streaming lendo apenas 3 colunas por shard."""
    history: pl.DataFrame | None = None
    for pq_path in parquet_files:
        shard_df = pl.read_parquet(pq_path, columns=["urna_serial", "file_seq", "raw_timestamp"])
        chunk_hist = extract_turn1_shard_history(shard_df)
        if not chunk_hist.is_empty():
            history = merge_turn1_history(history, chunk_hist)
        del shard_df, chunk_hist
    return history if history is not None else pl.DataFrame(schema=TURN1_HISTORY_SCHEMA)


def _normalize_turn1_history(turn1_history: pl.DataFrame | None) -> pl.DataFrame | None:
    if turn1_history is None or turn1_history.is_empty():
        return None
    return merge_turn1_history(None, turn1_history)


def strip_turn1_carryover(
    df: pl.DataFrame,
    *,
    election_year: int = 2026,
    turn: int,
    catalog: EventCatalog | None = None,
    turn1_history: pl.DataFrame | None = None,
    inter_turn_cutoff: datetime | None = None,
) -> tuple[pl.DataFrame, CarryoverFilterStats]:
    """Remove prefixo de carryover do 1º turno apenas quando turn == 2, preservando clock drift.

    Mantém 100% das linhas quando turn == 1. Quando turn == 2, resolve o início do 2º turno
    por arquivo `.jez` usando:
      1) Marcadores estruturais/ciclo de vida (turn2_init + boot_header) via `file_seq`
      2) Sobreposição de histórico do 1º turno por `urna_serial` (`t1_max_file_seq`)
      3) Fallback de data apenas sobre timestamps válidos sem `is_clock_drift`
    """
    raw_rows = df.height
    if raw_rows == 0:
        out_empty = (
            df
            if "is_clock_drift" in df.columns
            else df.with_columns(pl.lit(False).alias("is_clock_drift"))
        )
        return out_empty, CarryoverFilterStats(
            raw_rows=0,
            carryover_rows_dropped=0,
            silver_rows_kept=0,
            clock_drift_rows_retained=0,
            jez_files_total=0,
            jez_resolution_counts=_empty_resolution_counts(),
        )

    ts_dtype = df.schema.get("raw_timestamp")
    parsed_ts_expr = parse_bronze_timestamps_expr("raw_timestamp", dtype=ts_dtype)
    drift_expr = compute_clock_drift_expr(parsed_ts_expr, election_year)

    if turn != 2:
        out_t1 = (
            df
            if "is_clock_drift" in df.columns
            else df.with_columns(drift_expr.alias("is_clock_drift"))
        )
        drift_cnt = int(out_t1.select(pl.col("is_clock_drift").sum()).item() or 0)
        jez_total = int(out_t1.select(_jez_key_expr(out_t1.columns).n_unique()).item() or 0)
        return out_t1, CarryoverFilterStats(
            raw_rows=raw_rows,
            carryover_rows_dropped=0,
            silver_rows_kept=raw_rows,
            clock_drift_rows_retained=drift_cnt,
            jez_files_total=jez_total,
            jez_resolution_counts=_empty_resolution_counts(),
        )

    active_catalog = catalog if catalog is not None else _get_default_event_catalog()
    tbp = active_catalog.turn_boundary_patterns
    cutoff_dt = inter_turn_cutoff or default_inter_turn_cutoff(election_year)
    norm_hist = _normalize_turn1_history(turn1_history)
    has_history_source = norm_hist is not None

    has_msg = "message_text" in df.columns
    has_event_code = "event_code" in df.columns

    # Projeção mínima para fronteira (evita duplicar message_text e demais colunas em RAM)
    meta_exprs: list[pl.Expr] = [
        _jez_key_expr(df.columns).alias("_jez_key"),
        pl.col("urna_serial")
        .cast(pl.String)
        .str.strip_chars()
        .fill_null("UNKNOWN")
        .alias("_serial_norm"),
        pl.col("file_seq").cast(pl.Int64).alias("_file_seq_i64"),
        pl.col("raw_timestamp").cast(pl.String).alias("_raw_ts_str"),
        parsed_ts_expr.alias("_parsed_ts"),
        drift_expr.alias("_is_clock_drift"),
    ]

    if has_msg:
        msg_str = pl.col("message_text").cast(pl.String).fill_null("")
        meta_exprs.extend(
            [
                msg_str.str.contains(tbp.turn1_init).alias("_is_t1_init"),
                msg_str.str.contains(tbp.turn2_init).alias("_is_t2_init"),
                msg_str.str.contains(tbp.boot_header).alias("_is_boot"),
                msg_str.str.contains(tbp.turn1_close).alias("_is_t1_close"),
                msg_str.str.contains(_SESSION_ACTIVITY_REGEX).alias("_is_activity"),
            ]
        )
    elif has_event_code:
        ev_str = pl.col("event_code").cast(pl.String).fill_null("")
        meta_exprs.extend(
            [
                pl.lit(False).alias("_is_t1_init"),
                pl.lit(False).alias("_is_t2_init"),
                (ev_str == "LOGD_STARTED").alias("_is_boot"),
                (ev_str == "SECTION_CLOSED").alias("_is_t1_close"),
                ev_str.is_in(
                    ["SECTION_OPENED", "VOTER_ENABLED", "VOTER_VOTE_COMPUTED", "CONTINGENCY_EVENT"]
                ).alias("_is_activity"),
            ]
        )
    else:
        meta_exprs.extend(
            [
                pl.lit(False).alias("_is_t1_init"),
                pl.lit(False).alias("_is_t2_init"),
                pl.lit(False).alias("_is_boot"),
                pl.lit(False).alias("_is_t1_close"),
                pl.lit(False).alias("_is_activity"),
            ]
        )

    work = df.select(meta_exprs)

    if norm_hist is not None:
        work = work.join(
            norm_hist.rename({"urna_serial": "_serial_norm"}),
            on="_serial_norm",
            how="left",
        )
    else:
        work = work.with_columns(
            pl.lit(None, dtype=pl.Int64).alias("t1_max_file_seq"),
            pl.lit(None, dtype=pl.String).alias("t1_first_ts_raw"),
            pl.lit(None, dtype=pl.String).alias("t1_max_seq_ts_raw"),
        )

    non_drifted_ts = pl.when(pl.col("_parsed_ts").is_not_null() & ~pl.col("_is_clock_drift")).then(
        pl.col("_parsed_ts")
    )

    per_jez = work.group_by("_jez_key").agg(
        pl.col("_file_seq_i64").min().alias("_min_seq"),
        pl.col("_file_seq_i64").max().alias("_max_seq"),
        pl.col("_raw_ts_str").sort_by("_file_seq_i64").first().alias("_first_ts_raw"),
        pl.col("t1_max_file_seq").first().alias("_t1_max_seq"),
        pl.col("t1_first_ts_raw").first().alias("_t1_first_ts"),
        pl.col("t1_max_seq_ts_raw").first().alias("_t1_max_seq_ts"),
        pl.col("_raw_ts_str")
        .filter(pl.col("_file_seq_i64") == pl.col("t1_max_file_seq"))
        .first()
        .alias("_ts_at_t1_max"),
        pl.col("_is_t2_init")
        .filter(pl.col("_file_seq_i64") <= pl.col("t1_max_file_seq"))
        .any()
        .alias("_t2_init_le_t1_max"),
        pl.col("_is_t1_init").any().alias("_any_t1_init"),
        pl.col("_file_seq_i64").filter(pl.col("_is_t2_init")).min().alias("_any_t2_init_seq"),
        pl.col("_file_seq_i64")
        .filter(pl.col("_is_t2_init") & (pl.col("_file_seq_i64") > pl.col("t1_max_file_seq")))
        .min()
        .alias("_t2_init_after_t1_seq"),
        (non_drifted_ts < pl.lit(cutoff_dt)).any().fill_null(False).alias("_has_pre_cutoff"),
        (non_drifted_ts >= pl.lit(cutoff_dt)).any().fill_null(False).alias("_has_post_cutoff"),
        pl.col("_file_seq_i64")
        .filter(
            pl.col("_parsed_ts").is_not_null()
            & ~pl.col("_is_clock_drift")
            & (pl.col("_parsed_ts") < pl.lit(cutoff_dt))
        )
        .max()
        .alias("_last_pre_ts_seq"),
        pl.col("_file_seq_i64")
        .filter(
            pl.col("_parsed_ts").is_not_null()
            & ~pl.col("_is_clock_drift")
            & (pl.col("_parsed_ts") >= pl.lit(cutoff_dt))
        )
        .min()
        .alias("_first_post_ts_seq"),
    )

    # Identifica sobreposição real de Turno 1 vs Turno 2 (protegendo contra flash formatada)
    ts_fingerprint_present = (
        pl.col("_t1_first_ts").is_not_null() | pl.col("_t1_max_seq_ts").is_not_null()
    )
    ts_fingerprint_match = (pl.col("_first_ts_raw") == pl.col("_t1_first_ts")).fill_null(False) | (
        pl.col("_ts_at_t1_max") == pl.col("_t1_max_seq_ts")
    ).fill_null(False)
    per_jez = per_jez.with_columns(
        (pl.col("_t1_max_seq").is_not_null() & (pl.col("_min_seq") > pl.col("_t1_max_seq"))).alias(
            "_already_past_t1"
        ),
        (
            pl.col("_t1_max_seq").is_not_null()
            & (pl.col("_min_seq") <= pl.col("_t1_max_seq"))
            & (pl.col("_max_seq") > pl.col("_t1_max_seq"))
            & pl.when(ts_fingerprint_present)
            .then(ts_fingerprint_match)
            .otherwise(~pl.col("_t2_init_le_t1_max").fill_null(False))
        ).alias("_has_t1_overlap"),
    ).with_columns(
        pl.when(pl.col("_has_t1_overlap"))
        .then(pl.col("_t2_init_after_t1_seq"))
        .otherwise(pl.col("_any_t2_init_seq"))
        .alias("_first_t2_init_seq")
    )

    # 2º passe compacto por _jez_key para ancorar no último evento de T1 e primeiro boot de T2
    anchor_cols = [
        "_jez_key",
        "_file_seq_i64",
        "_is_t1_close",
        "_is_t1_init",
        "_is_boot",
        "_is_activity",
        "t1_max_file_seq",
    ]
    work_thin = work.select(anchor_cols)
    drift_series = work.get_column("_is_clock_drift")
    jez_key_series = work.get_column("_jez_key")
    file_seq_series = work.get_column("_file_seq_i64")
    del work

    work_anchors = work_thin.join(
        per_jez.select(["_jez_key", "_first_t2_init_seq", "_first_post_ts_seq"]),
        on="_jez_key",
        how="left",
    )

    per_jez_anchors = work_anchors.group_by("_jez_key").agg(
        pl.col("_file_seq_i64")
        .filter(pl.col("_is_t1_close") & (pl.col("_file_seq_i64") < pl.col("_first_t2_init_seq")))
        .max()
        .alias("_last_t1_close_before_t2"),
        pl.col("_file_seq_i64")
        .filter(pl.col("_is_t1_init") & (pl.col("_file_seq_i64") < pl.col("_first_t2_init_seq")))
        .max()
        .alias("_last_t1_init_before_t2"),
        pl.col("_file_seq_i64")
        .filter(pl.col("_is_boot") & (pl.col("_file_seq_i64") <= pl.col("_first_t2_init_seq")))
        .max()
        .alias("_boot_le_t2"),
        pl.col("_file_seq_i64")
        .filter(pl.col("_is_boot") & (pl.col("_file_seq_i64") > pl.col("t1_max_file_seq")))
        .min()
        .alias("_first_boot_after_t1"),
        pl.col("_file_seq_i64")
        .filter(pl.col("_is_boot") & (pl.col("_file_seq_i64") <= pl.col("_first_post_ts_seq")))
        .max()
        .alias("_boot_le_post_ts"),
    )
    del work_anchors

    # Verifica se há atividade de sessão entre t1_max_file_seq e _first_boot_after_t1
    per_jez_gap = (
        work_thin.select(["_jez_key", "_file_seq_i64", "_is_activity", "t1_max_file_seq"])
        .join(
            per_jez_anchors.select(["_jez_key", "_first_boot_after_t1"]),
            on="_jez_key",
            how="left",
        )
        .group_by("_jez_key")
        .agg(
            pl.col("_is_activity")
            .filter(
                (pl.col("_file_seq_i64") > pl.col("t1_max_file_seq"))
                & (pl.col("_file_seq_i64") < pl.col("_first_boot_after_t1"))
            )
            .any()
            .fill_null(False)
            .alias("_activity_before_boot_after_t1"),
        )
    )
    del work_thin

    resolved_jez = per_jez.join(per_jez_anchors, on="_jez_key", how="left").join(
        per_jez_gap, on="_jez_key", how="left"
    )
    del per_jez, per_jez_anchors, per_jez_gap

    last_t1_anchor = pl.max_horizontal(
        pl.lit(0, dtype=pl.Int64),
        pl.when(pl.col("_has_t1_overlap") | pl.col("_already_past_t1"))
        .then(pl.col("_t1_max_seq"))
        .otherwise(pl.lit(0, dtype=pl.Int64)),
        pl.col("_last_t1_close_before_t2").fill_null(0),
        pl.col("_last_t1_init_before_t2").fill_null(0),
    )

    snapped_lifecycle_seq = (
        pl.when(pl.col("_boot_le_t2").is_not_null() & (pl.col("_boot_le_t2") > last_t1_anchor))
        .then(pl.col("_boot_le_t2"))
        .otherwise(pl.col("_first_t2_init_seq"))
    )

    seq_overlap_keep = (
        pl.when(
            pl.col("_first_boot_after_t1").is_not_null()
            & ~pl.col("_activity_before_boot_after_t1").fill_null(False)
        )
        .then(pl.col("_first_boot_after_t1"))
        .otherwise(pl.col("_t1_max_seq") + 1)
    )

    snapped_date_seq = (
        pl.when(
            pl.col("_boot_le_post_ts").is_not_null()
            & (pl.col("_boot_le_post_ts") > pl.col("_last_pre_ts_seq").fill_null(0))
        )
        .then(pl.col("_boot_le_post_ts"))
        .otherwise(pl.col("_first_post_ts_seq"))
    )

    resolved_jez = resolved_jez.with_columns(
        pl.when(pl.col("_first_t2_init_seq").is_not_null())
        .then(
            pl.when(snapped_lifecycle_seq <= pl.col("_min_seq"))
            .then(pl.col("_min_seq"))
            .otherwise(snapped_lifecycle_seq)
        )
        .when(pl.col("_has_t1_overlap"))
        .then(seq_overlap_keep)
        .when(pl.col("_already_past_t1"))
        .then(pl.col("_min_seq"))
        .when(pl.col("_has_pre_cutoff") & pl.col("_has_post_cutoff"))
        .then(snapped_date_seq)
        .otherwise(pl.col("_min_seq"))
        .cast(pl.Int64)
        .alias("_keep_from_seq"),
        pl.when(pl.col("_first_t2_init_seq").is_not_null())
        .then(
            pl.when(snapped_lifecycle_seq <= pl.col("_min_seq"))
            .then(pl.lit("fresh_urna_no_carryover"))
            .otherwise(pl.lit("lifecycle_marker"))
        )
        .when(pl.col("_has_t1_overlap"))
        .then(pl.lit("turn1_urna_seq_overlap"))
        .when(pl.col("_already_past_t1"))
        .then(pl.lit("fresh_urna_no_carryover"))
        .when(pl.col("_has_pre_cutoff") & pl.col("_has_post_cutoff"))
        .then(pl.lit("date_fallback"))
        .when(pl.col("_any_t1_init") | pl.col("_has_pre_cutoff"))
        .then(pl.lit("unresolved"))
        .when(
            pl.col("_has_post_cutoff")
            | (pl.lit(has_history_source) & pl.col("_t1_max_seq").is_null())
        )
        .then(pl.lit("fresh_urna_no_carryover"))
        .otherwise(pl.lit("unresolved"))
        .alias("_resolution_method"),
    )

    method_counts = _empty_resolution_counts()
    for row in (
        resolved_jez.group_by("_resolution_method").agg(pl.len().alias("cnt")).iter_rows(named=True)
    ):
        method_name = str(row["_resolution_method"])
        if method_name in method_counts:
            method_counts[method_name] = int(row["cnt"])

    jez_total = resolved_jez.height

    keep_mask_df = (
        pl.DataFrame({"_jez_key": jez_key_series, "_file_seq_i64": file_seq_series})
        .join(
            resolved_jez.select(["_jez_key", "_keep_from_seq"]),
            on="_jez_key",
            how="left",
            maintain_order="left",
        )
        .select((pl.col("_file_seq_i64") >= pl.col("_keep_from_seq")).alias("_keep"))
    )
    keep_mask = keep_mask_df.get_column("_keep")
    del keep_mask_df, jez_key_series, file_seq_series, resolved_jez

    if "is_clock_drift" not in df.columns:
        result_df = df.with_columns(drift_series.alias("is_clock_drift")).filter(keep_mask)
    else:
        result_df = df.filter(keep_mask)
    del drift_series, keep_mask

    silver_kept = result_df.height
    drift_kept = int(result_df.select(pl.col("is_clock_drift").sum()).item() or 0)

    return result_df, CarryoverFilterStats(
        raw_rows=raw_rows,
        carryover_rows_dropped=raw_rows - silver_kept,
        silver_rows_kept=silver_kept,
        clock_drift_rows_retained=drift_kept,
        jez_files_total=jez_total,
        jez_resolution_counts=method_counts,
    )


def _col_or_default(
    df: pl.DataFrame,
    name: str,
    default_val: object,
    dtype: pl.DataType,
    aliases: tuple[str, ...] = (),
) -> pl.Expr:
    cols = set(df.columns)
    for src_name in (name, *aliases):
        if src_name in cols:
            return pl.col(src_name).cast(dtype)
    return pl.lit(default_val, dtype=dtype).alias(name)


def transform_bronze_shard_to_silver(
    bronze_df: pl.DataFrame,
    *,
    election_year: int = 2026,
    turn: int,
    uf: str,
    catalog: EventCatalog | None = None,
    turn1_history: pl.DataFrame | None = None,
    inter_turn_cutoff: datetime | None = None,
    parser_version: str = SILVER_PARSER_VERSION,
) -> tuple[pl.DataFrame, CarryoverFilterStats]:
    """Converte um shard Bronze de fact_urna_logs para o schema Silver fact_urna_event."""
    active_catalog = catalog if catalog is not None else _get_default_event_catalog()

    stripped_df, stats = strip_turn1_carryover(
        bronze_df,
        election_year=election_year,
        turn=turn,
        catalog=active_catalog,
        turn1_history=turn1_history,
        inter_turn_cutoff=inter_turn_cutoff,
    )

    if stripped_df.is_empty():
        return pl.DataFrame(schema=SILVER_FACT_URNA_EVENT_SCHEMA), stats

    ts_dtype = stripped_df.schema.get("raw_timestamp")
    parsed_ts_expr = parse_bronze_timestamps_expr("raw_timestamp", dtype=ts_dtype)

    has_app = "application" in stripped_df.columns
    has_msg = "message_text" in stripped_df.columns
    app_norm_expr = (
        pl.col("application").cast(pl.String).fill_null("VOTA").replace("", "VOTA")
        if has_app
        else pl.lit("VOTA", dtype=pl.String)
    )
    msg_norm_expr = (
        pl.col("message_text").cast(pl.String).fill_null("")
        if has_msg
        else pl.lit("", dtype=pl.String)
    )

    # Projeção mínima para regressão temporal (evita ordenar todas as colunas em RAM)
    ts_reg_df = (
        stripped_df.select(
            [
                _jez_key_expr(stripped_df.columns).alias("_jez_key"),
                pl.col("urna_serial").cast(pl.String).fill_null("UNKNOWN").alias("_serial_norm"),
                pl.col("file_seq").cast(pl.Int64).alias("_file_seq_i64"),
                parsed_ts_expr.alias("_parsed_ts"),
            ]
        )
        .with_row_index("_out_idx")
        .sort(["_jez_key", "_file_seq_i64"])
    )

    prev_valid_ts = pl.col("_parsed_ts").forward_fill().shift(1).over(["_jez_key", "_serial_norm"])
    ts_reg_series = (
        ts_reg_df.select(
            [
                pl.col("_out_idx"),
                (
                    pl.col("_parsed_ts").is_not_null()
                    & prev_valid_ts.is_not_null()
                    & (pl.col("_parsed_ts") < prev_valid_ts)
                )
                .fill_null(False)
                .alias("_has_ts_regression"),
            ]
        )
        .sort("_out_idx")
        .get_column("_has_ts_regression")
    )
    del ts_reg_df

    # Classificação deduplicada por (application, message_text) único no shard
    pair_df = stripped_df.select(
        [
            app_norm_expr.alias("_app_norm"),
            msg_norm_expr.alias("_msg_norm"),
        ]
    )
    unique_pairs = pair_df.unique()
    apps_u: list[str] = []
    msgs_u: list[str] = []
    codes_u: list[str] = []
    bio_u: list[int | None] = []
    statuses_u: list[str] = []

    fallback_codes = {active_catalog.fallback_event_code, active_catalog.malformed_event_code}
    for app_val, msg_val in unique_pairs.iter_rows():
        res = active_catalog.classify(msg_val, app_val)
        apps_u.append(app_val)
        msgs_u.append(msg_val)
        codes_u.append(res.event_code)
        bio_u.append(res.biometric_attempt_num)
        statuses_u.append("UNKNOWN" if res.event_code in fallback_codes else "CLASSIFIED")
    del unique_pairs

    lookup_df = pl.DataFrame(
        {
            "_app_norm": pl.Series(apps_u, dtype=pl.String),
            "_msg_norm": pl.Series(msgs_u, dtype=pl.String),
            "event_code": pl.Series(codes_u, dtype=pl.String),
            "biometric_attempt_num": pl.Series(bio_u, dtype=pl.Int8),
            "classification_status": pl.Series(statuses_u, dtype=pl.String),
        }
    )

    classified_cols = pair_df.join(
        lookup_df,
        on=["_app_norm", "_msg_norm"],
        how="left",
        maintain_order="left",
    ).select(["_app_norm", "event_code", "biometric_attempt_num", "classification_status"])
    del pair_df, lookup_df

    silver_df = stripped_df.select(
        [
            _col_or_default(stripped_df, "election_year", election_year, pl.Int16).alias(
                "election_year"
            ),
            _col_or_default(stripped_df, "turn", turn, pl.Int8).alias("turn"),
            _col_or_default(stripped_df, "uf", uf, pl.String).alias("uf"),
            _col_or_default(
                stripped_df, "municipio_code", None, pl.Int32, aliases=("cd_municipio",)
            ).alias("municipio_code"),
            _col_or_default(stripped_df, "zone_num", None, pl.Int16, aliases=("nr_zona",)).alias(
                "zone_num"
            ),
            _col_or_default(
                stripped_df, "section_num", None, pl.Int32, aliases=("nr_secao",)
            ).alias("section_num"),
            _col_or_default(stripped_df, "urna_serial", None, pl.String).alias("urna_serial"),
            pl.col("file_seq").cast(pl.Int64).alias("file_seq"),
            parsed_ts_expr.alias("raw_timestamp"),
            pl.col("is_clock_drift").cast(pl.Boolean).alias("is_clock_drift"),
            pl.lit(False, dtype=pl.Boolean).alias("has_timestamp_regression"),
            pl.lit("VOTA", dtype=pl.String).alias("application"),
            _col_or_default(stripped_df, "log_level", None, pl.String).alias("log_level"),
        ]
    ).with_columns(
        ts_reg_series.cast(pl.Boolean).alias("has_timestamp_regression"),
        classified_cols.get_column("_app_norm").alias("application"),
        classified_cols.get_column("event_code").cast(pl.String).alias("event_code"),
        classified_cols.get_column("biometric_attempt_num")
        .cast(pl.Int8)
        .alias("biometric_attempt_num"),
        classified_cols.get_column("classification_status")
        .cast(pl.String)
        .alias("classification_status"),
        pl.lit(parser_version, dtype=pl.String).alias("parser_version"),
    ).select(list(SILVER_FACT_URNA_EVENT_SCHEMA.names()))
    del stripped_df, ts_reg_series, classified_cols

    return silver_df, stats
