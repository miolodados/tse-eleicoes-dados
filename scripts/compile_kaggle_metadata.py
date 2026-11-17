#!/usr/bin/env python3
"""Compilador de metadados do dataset no Kaggle (pt-BR).

Lê os arquivos de configuração declarativos em configs/kaggle_metadata/ e gera:
1. data/kaggle_metadata/dataset-metadata.json (pronto para kaggle datasets metadata --update)
2. DICIONARIO_DE_DADOS.md (documentação detalhada para pesquisadores)
3. LEIAME.md (guia de uso rápido com DuckDB e Polars)
"""

import json
from pathlib import Path

import yaml


def build_markdown_description(dataset_cfg: dict, camadas_cfg: dict, tables: dict) -> str:
    """Gera a descrição Markdown rica em pt-BR para a interface do Kaggle."""
    lines = []

    lines.append(f"# {dataset_cfg['title']}")
    lines.append("")
    lines.append(f"> **{dataset_cfg['subtitle']}**")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 1. Visão Geral e Contexto")
    lines.append("")
    lines.append(
        "Este acervo disponibiliza os dados oficiais das eleições brasileiras de 2026 "
        "(e pleitos históricos correlatos) publicados pelo **Tribunal Superior Eleitoral (TSE)**, "
        "estruturados em uma **arquitetura de medalhão (Medallion Architecture)** moderna com "
        "camadas **Bronze**, **Silver** e **Gold**."
    )
    lines.append("")
    lines.append(
        "Todos os dados foram convertidos do formato bruto para **Apache Parquet colunar** "
        "com compressão **Zstandard (ZSTD nível 5)** e particionamento Hive "
        "(`election_year=YYYY/turn=T/uf=UF`), viabilizando consultas analíticas "
        "com motores como **DuckDB**, **Polars** e **Apache Arrow** diretamente sobre o acervo."
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 2. Arquitetura de Camadas (Medallion Architecture)")
    lines.append("")
    lines.append("| Camada | Nome | Propósito | Formato / Particionamento |")
    lines.append("|---|---|---|---|")
    lines.append(
        "| 🥉 **Bronze** | Ingestão Fiel 1:1 | "
        "Espelhamento exato das bases do TSE com sanitização de PII | "
        "Parquet ZSTD (Hive: election_year/turn/uf) |"
    )
    lines.append(
        "| 🥈 **Silver** | Conformada & Semântica | "
        "Classificação semântica de eventos, sessões e dimensões | "
        "Parquet ZSTD com controle de versão |"
    )
    lines.append(
        "| 🥇 **Gold** | Analítica de Negócio | "
        "Tabelas analíticas pré-agregadas para dashboards e estudos | "
        "Parquet ZSTD consolidado |"
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 3. Catálogo de Tabelas por Camada")
    lines.append("")

    layers_info = [
        ("gold", "Camada Gold (Analítica)"),
        ("silver", "Camada Silver (Conformada)"),
        ("bronze", "Camada Bronze (Ingestão Fiel)"),
    ]

    for idx, (layer_key, layer_name) in enumerate(layers_info, start=1):
        layer_tables = [t for t in tables.values() if t.get("camada") == layer_key]
        lines.append(f"### 3.{idx} {layer_name}")
        lines.append("")
        lines.append("| Tabela | Grão | Chaves Naturais / Primárias | Descrição |")
        lines.append("|---|---|---|---|")
        for t in sorted(layer_tables, key=lambda x: x["tabela"]):
            keys = ", ".join(f"`{k}`" for k in t.get("chaves_naturais", [])[:4])
            lines.append(
                f"| `{t['tabela']}` | {t.get('grao', '')} | {keys} | {t.get('descricao', '')} |"
            )
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append("## 4. Salvaguardas de Privacidade e Conformidade LGPD")
    lines.append("")
    lines.append(
        "- **Zero PII (Personally Identifiable Information):** Nenhum identificador individual "
        "de eleitores ou mesários é exposto. Números com 5 ou mais dígitos presentes em logs "
        "foram deterministicamente mascarados como `<ID_REDACTED>`."
    )
    lines.append(
        "- **Remoção de Entropia Hexadecimal:** O campo de autenticação (`mac_code`) de logs "
        "foi omitido na persistência, reduzindo entropia incompressível sem prejuízo ao valor."
    )
    lines.append(
        "- **Amostragem Representativa:** A tabela `fact_urna_logs` contém uma amostra técnica "
        "de até 5 arquivos `.jez` por partição para viabilizar exploração analítica no Kaggle."
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 5. Exemplos de Consulta")
    lines.append("")
    lines.append("### 5.1 Consulta Analítica com DuckDB (Python / SQL)")
    lines.append("```python")
    lines.append("import duckdb")
    lines.append("")
    lines.append("# Consulta analítica direta sobre Parquet na camada Gold")
    lines.append("conn = duckdb.connect()")
    lines.append("query = '''")
    lines.append("    SELECT event_code, SUM(event_count) as total_eventos")
    lines.append("    FROM read_parquet('/kaggle/input/tse-analysis-eleicoes-2026/gold/*.parquet')")
    lines.append("    GROUP BY event_code")
    lines.append("    ORDER BY total_eventos DESC")
    lines.append("    LIMIT 10")
    lines.append("'''")
    lines.append("df_events = conn.execute(query).pl()")
    lines.append("print(df_events)")
    lines.append("```")
    lines.append("")
    lines.append("### 5.2 Consulta Escalar com Polars")
    lines.append("```python")
    lines.append("import polars as pl")
    lines.append("")
    lines.append("# Leitura lazy e agregação sobre fact_voting_session")
    lines.append(
        "uri = '/kaggle/input/tse-analysis-eleicoes-2026/silver/fact_voting_session/**/*.parquet'"
    )
    lines.append("sessions = pl.scan_parquet(uri)")
    lines.append("resumo = sessions.group_by('final_state').agg(")
    lines.append("    pl.len().alias('total_urnas'),")
    lines.append("    pl.col('voter_cycles').sum().alias('total_votos')")
    lines.append(").collect()")
    lines.append("print(resumo)")
    lines.append("```")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 6. Proveniência e Licença")
    lines.append("")
    inst = dataset_cfg["provenance"]["source_institution"]
    portal = dataset_cfg["provenance"]["official_portal"]
    curator = dataset_cfg["provenance"]["curator"]
    repo = dataset_cfg["provenance"]["repository"]
    lic = dataset_cfg["license"]
    lines.append(f"- **Fonte Primária Oficial:** [{inst}]({portal})")
    lines.append(f"- **Repositório do Projeto:** [{curator}]({repo})")
    lines.append(
        f"- **Licença Declarada:** **{lic}** (Creative Commons Atribuição 4.0 Internacional). "
        "Exige atribuição aos dados abertos do TSE e ao projeto miolodados."
    )
    lines.append("")

    return "\n".join(lines)


def main():
    repo_root = Path(__file__).resolve().parent.parent
    meta_dir = repo_root / "configs" / "kaggle_metadata"
    tables_dir = meta_dir / "tabelas"

    with open(meta_dir / "dataset.yml", encoding="utf-8") as f:
        dataset_cfg = yaml.safe_load(f)

    with open(meta_dir / "camadas.yml", encoding="utf-8") as f:
        camadas_cfg = yaml.safe_load(f)

    tables = {}
    for yml in sorted(tables_dir.glob("*.yml")):
        with open(yml, encoding="utf-8") as f:
            t_data = yaml.safe_load(f)
            tables[t_data["tabela"]] = t_data

    description_md = build_markdown_description(dataset_cfg, camadas_cfg, tables)

    metadata_json = {
        "title": dataset_cfg["title"],
        "subtitle": dataset_cfg["subtitle"],
        "id": dataset_cfg["slug"],
        "licenses": [{"name": dataset_cfg["license"]}],
        "description": description_md,
        "isPrivate": dataset_cfg["is_private"],
        "expectedUpdateFrequency": dataset_cfg["expected_update_frequency"],
        "keywords": dataset_cfg["keywords"],
        "collaborators": dataset_cfg.get("collaborators", []),
    }

    out_dir = repo_root / "data" / "kaggle_metadata"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_json = out_dir / "dataset-metadata.json"

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(metadata_json, f, indent=2, ensure_ascii=False)

    # Also persist in configs/kaggle_metadata for repository versioning
    repo_json = meta_dir / "dataset-metadata.json"
    with open(repo_json, "w", encoding="utf-8") as f:
        json.dump(metadata_json, f, indent=2, ensure_ascii=False)

    print(f"Sucesso: gerado {out_json} e {repo_json} ({out_json.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()
