# tse-eleicoes-dados — Dados eleitorais públicos do TSE

Produto de dados do Miolo Dados construído a partir de fontes públicas do Tribunal Superior Eleitoral (TSE), com foco inicial nas Eleições 2026 e possibilidade de expansão parametrizada para outros pleitos.

O projeto é estruturado em uma arquitetura de dados em três camadas: **Bronze, Silver e Gold**, com execução e computação integralmente no **Kaggle**. Os contratos e diretrizes estão descritos em [docs/data_architecture.md](docs/data_architecture.md) e no [dicionário de dados](docs/data_dictionary.md).

---

## 1. Arquitetura em Camadas

```
[Fontes Oficiais TSE (CKAN/CDN)]
               │
               ▼
   ┌──────────────────────┐
   │     Camada Bronze    │  18 tabelas 1:1 particionadas em Parquet ZSTD
   │ (Dados Brutos 1:1)   │  (logs da urna com sanitização zero-PII, boletins, apuração, eleitorado)
   └──────────┬───────────┘
               │
               ▼
   ┌──────────────────────┐
   │     Camada Silver    │  Eventos classificados (`fact_urna_event`), sessões reconstruídas
   │ (Fatos e Dimensões)  │  (`fact_voting_session`) e dimensões conformadas (`dim_election`, `dim_cargo`, `dim_partido`)
   └──────────┬───────────┘
               │
               ▼
   ┌──────────────────────┐
   │     Camada Gold      │  Agregações analíticas e operacionais: distribuição de eventos, completude
   │  (Métricas e KPIs)   │  de sessões e análise de tentativas biométricas
   └──────────┬───────────┘
               │
               ▼
   [Dataset Oficial Kaggle: miolodados/tse-eleicoes-dados]
               ▲
               │ (leitura/consumo)
         [TS-Zé & Analistas]
```

| Camada | Responsabilidade | Tabelas | Status |
|---|---|---|---|
| **Bronze** | Preservar dados das fontes oficiais em grão nativo 1:1, com salvaguardas de privacidade (mascaramento de identificadores de 5+ dígitos como `<ID_REDACTED>` e omissão de `mac_code`). | 18 tabelas (eletorais, logs, comparecimento, correspondência) | Implementada e auditada |
| **Silver** | Classificar eventos via catálogo declarativo (`fact_urna_event`), reconstruir sessões de votação via máquina de estados (`fact_voting_session`) e conformar dimensões (`dim_election`, `dim_cargo`, `dim_partido`). | 5 tabelas | Implementada e validada |
| **Gold** | Publicar produtos analíticos com métricas operacionais auditadas (`gold_event_distribution`, `gold_session_completeness`, `gold_biometric_attempts`). | 3 produtos | Implementada e validada |

---

## 2. Notebooks do Pipeline (Execução 100% no Kaggle)

Todos os notebooks são autocontidos, idempotentes e executados remotamente em Kaggle Kernels usando a GPU/CPU da plataforma:

| Notebook | Camada | Objetivo | Kernel Kaggle |
|---|---|---|---|
| `01_urna_logs_poc` | PoC | PoC de streaming e parser de `.jez` em memória | `daniellmattos/tse-analysis-01-urna-logs-poc` |
| `02_ingest_electoral_tables` | Bronze | Ingestão das 17 tabelas tabulares 1:1 em Parquet ZSTD | `daniellmattos/tse-analysis-02-ingest-electoral-tables` |
| `03_ingest_urna_logs_batch` | Bronze | Ingestão em batch particionado dos logs da urna (`fact_urna_logs`) | `daniellmattos/tse-analysis-03-ingest-urna-logs-batch` |
| `04_dataset_integrity_and_release` | Bronze | Auditoria de integridade, cobertura e integridade de hashes | `daniellmattos/tse-analysis-04-dataset-integrity-and-release` |
| `05_silver_fact_urna_event` | Silver | Classificação semântica dos logs da urna em 16 event codes canônicos | `daniellmattos/tse-analysis-05-silver-fact-urna-event` |
| `06_silver_fact_voting_session` | Silver | Reconstrução de sessões operacionais de votação via máquina de estados v1.0.0 | `daniellmattos/tse-analysis-06-silver-fact-voting-session` |
| `07_silver_dimensions` | Silver | Extração e conformação de dimensões (`dim_election`, `dim_cargo`, `dim_partido`) | `daniellmattos/tse-analysis-07-silver-dimensions` |
| `08_gold_analytics` | Gold | Cálculo de distribuições de eventos, completude de sessões e biometria | `daniellmattos/tse-analysis-08-gold-analytics` |
| `09_release_pipeline` | Release | Auditoria multi-camada (26 tabelas), reconciliação e geração do `manifest.json` | `daniellmattos/tse-analysis-09-release-pipeline` |

---

## 3. Execução e Atualização via Kaggle CLI

A execução de ponta a ponta e a publicação de versões ocorrem exclusivamente no Kaggle.

> **Não use** `kaggle datasets version ... --dir-mode zip` neste dataset: ele compacta cada pasta em `.zip`, destrói a estrutura Hive (`election_year=/turn=/uf=`) e remove a navegação de arquivos e colunas no Kaggle. Novas versões de dados devem preservar a árvore de arquivos (procedimento em validação).

```bash
# 1. Executar notebook individual via Kaggle CLI
cd notebooks/05_silver_fact_urna_event
kaggle kernels push -p .
kaggle kernels status daniellmattos/tse-analysis-05-silver-fact-urna-event

# 2. Executar pipeline de validação final
cd notebooks/09_release_pipeline
kaggle kernels push -p .
kaggle kernels status daniellmattos/tse-analysis-09-release-pipeline

# 3. Atualizar SOMENTE os metadados do dataset (sem reenviar os dados)
kaggle datasets metadata --update -p <pasta_com_dataset-metadata.json> <dono>/<dataset>
```

---

## 4. Dataset no Kaggle e Governança

- **Identificador Canônico:** `miolodados/tse-eleicoes-dados`
- **Identificador Legado (origem):** `daniellmattos/tse-analysis-eleicoes-2026` (mantido como referência de transição).
- **Consumidores:** Consultam as camadas publicadas diretamente via DuckDB, Polars ou PyArrow; não implementam rotinas de ETL. O TS-Zé consome as tabelas publicadas desta base.

### Licença e Fontes

As fontes são dados públicos abertos disponibilizados pelo TSE. Todas as 7 famílias de fontes estão mapeadas em `configs/sources.yml` com `redistribution_allowed: true`, inclusive Comparecimento e Abstenção do Sistema ELO.

---

## 5. Estrutura do Repositório

```text
tse-eleicoes-dados/
├── configs/
│   ├── event_catalog.yml       # Allowlist semântica e regras de classificação de eventos
│   └── sources.yml             # Catálogo oficial das fontes primárias do TSE
├── docs/
│   ├── data_architecture.md    # Arquitetura e responsabilidades Bronze/Silver/Gold
│   ├── source_inventory.md     # Inventário técnico detalhado das fontes
│   └── data_dictionary.md      # Dicionário de schemas e relacionamentos
├── notebooks/
│   ├── 01_urna_logs_poc/
│   ├── 02_ingest_electoral_tables/
│   ├── 03_ingest_urna_logs_batch/
│   ├── 04_dataset_integrity_and_release/
│   ├── 05_silver_fact_urna_event/
│   ├── 06_silver_fact_voting_session/
│   ├── 07_silver_dimensions/
│   ├── 08_gold_analytics/
│   └── 09_release_pipeline/
├── src/
│   └── tse_analysis/           # Módulos core de inspeção, catálogo e materialização
└── tests/                      # 135+ testes automatizados (unitários e adversariais)
```

---

## 6. Ambiente de Desenvolvimento

```bash
# Instalação do ambiente com uv
uv venv
uv pip install -e ".[dev]"

# Execução da suíte completa de testes e linters
uv run pytest -q
uv run ruff check .
```
