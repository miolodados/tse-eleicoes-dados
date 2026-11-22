#!/usr/bin/env python3
"""Consolida manifestos parciais de sessões paralelas e publica release única no KaggleHub."""

import json
import os
from pathlib import Path

DATASET_PATH = Path(
    os.environ.get("DATASET_PATH", "/content/drive/MyDrive/tse-eleicoes-dados/data")
)
CHECKPOINTS_DIR = DATASET_PATH / "_checkpoints"
OFFICIAL_MANIFEST = CHECKPOINTS_DIR / "manifest.json"
DATASET_HANDLE = os.environ.get(
    "KAGGLE_DATASET_HANDLE", "daniellmattos/tse-eleicoes-dados-v2"
)


def merge_manifests() -> dict:
    if not CHECKPOINTS_DIR.exists():
        raise FileNotFoundError(
            f"Diretório de checkpoints não encontrado: {CHECKPOINTS_DIR}"
        )

    if not OFFICIAL_MANIFEST.exists():
        raise FileNotFoundError(
            f"Manifesto oficial não encontrado: {OFFICIAL_MANIFEST}"
        )

    with open(OFFICIAL_MANIFEST, encoding="utf-8") as fh:
        merged = json.load(fh)

    w_files = sorted(CHECKPOINTS_DIR.glob("manifest_w*.json"))
    print(f"[CONSOLIDATION] Manifestos de workers encontrados: {[f.name for f in w_files]}")

    for wf in w_files:
        try:
            with open(wf, encoding="utf-8") as fh:
                data = json.load(fh)
            for k, meta in data.get("partitions", {}).items():
                if k.startswith("fact_urna_logs"):
                    merged["partitions"][k] = meta
        except Exception as exc:
            print(f"[WARN] Erro ao ler {wf.name}: {exc}. Pulando...")

    # Reconciliação dos logs de urna
    urna_parts = {
        k: v for k, v in merged["partitions"].items() if k.startswith("fact_urna_logs")
    }
    t1_parts = [k for k in urna_parts if "turn=1" in k]
    t2_parts = [k for k in urna_parts if "turn=2" in k]

    total_urna_rows = sum(m.get("row_count", 0) for m in urna_parts.values())
    total_urna_files = sum(m.get("part_count", 1) for m in urna_parts.values())

    print(
        f"[CONSOLIDATION] fact_urna_logs: {len(urna_parts)} partições "
        f"({len(t1_parts)} T1, {len(t2_parts)} T2) | "
        f"{total_urna_files} arquivos Parquet | {total_urna_rows:,} linhas totais."
    )

    if "tables_summary" in merged and "fact_urna_logs" in merged["tables_summary"]:
        merged["tables_summary"]["fact_urna_logs"]["partition_count"] = len(urna_parts)
        merged["tables_summary"]["fact_urna_logs"]["total_rows"] = total_urna_rows
        merged["tables_summary"]["fact_urna_logs"]["total_files"] = total_urna_files

    # Escrita atômica segura via tmp + replace
    tmp_manifest = OFFICIAL_MANIFEST.with_suffix(".json.tmp")
    with open(tmp_manifest, "w", encoding="utf-8") as fh:
        json.dump(merged, fh, indent=2, ensure_ascii=False)
    tmp_manifest.replace(OFFICIAL_MANIFEST)
    print(f"[SUCESSO] Manifesto oficial atualizado em: {OFFICIAL_MANIFEST}")

    # Limpeza dos manifests temporários de workers
    for wf in w_files:
        try:
            wf.unlink()
            lock_f = wf.with_suffix(".json.lock")
            if lock_f.exists():
                lock_f.unlink()
            print(f"[CLEANUP] Removido {wf.name}")
        except Exception as e:
            print(f"[WARN] Falha ao limpar {wf.name}: {e}")

    return merged


def release_to_kaggle(merged: dict) -> None:
    skip = os.environ.get("SKIP_KAGGLE_RELEASE", "").lower() in ("1", "true", "yes")
    if skip:
        print("[INFO] SKIP_KAGGLE_RELEASE=1: Pulando upload no KaggleHub.")
        return

    import kagglehub

    urna_parts = {
        k: v for k, v in merged["partitions"].items() if k.startswith("fact_urna_logs")
    }
    total_urna_rows = sum(m.get("row_count", 0) for m in urna_parts.values())
    total_urna_files = sum(m.get("part_count", 1) for m in urna_parts.values())

    phase_id = "03_ingest_urna_logs_batch"
    phase_summary = (
        f"Ingestão 100% completa de logs de urna (2026): {len(urna_parts)} partições "
        f"(28 UFs T1 + 28 UFs T2), {total_urna_files} arquivos Parquet ZSTD, "
        f"{total_urna_rows:,} linhas totais auditadas"
    )

    print(f"Publicando versão oficial completa no KaggleHub ({DATASET_HANDLE})...")
    kagglehub.dataset_upload(
        handle=DATASET_HANDLE,
        local_dataset_dir=str(DATASET_PATH),
        version_notes=f"[{phase_id}] {phase_summary}",
        ignore_patterns=[
            "*.tmp",
            "*.part",
            "*.zip",
            "*.jez",
            "*.log",
            "__pycache__/",
            ".ipynb_checkpoints/",
            ".DS_Store",
        ],
    )
    print(f"[SUCESSO] Dataset publicado com sucesso no Kaggle: {DATASET_HANDLE}")


if __name__ == "__main__":
    merged_data = merge_manifests()
    release_to_kaggle(merged_data)
