# Pipeline Notebooks (`notebooks/`)

Este diretório contém os **9 notebooks autocontidos e idempotentes** do lakehouse eleitoral do TSE (camadas **Bronze**, **Silver** e **Gold**), projetados para execução reproduzível no **Google Colab** (Web UI ou `colab` CLI) e no **Kaggle Notebooks**.

---

## 1. Estrutura do Diretório

Cada subpasta contém exclusivamente o notebook `.ipynb` e seu respectivo `kernel-metadata.json`. Cópias intermediárias (`.py`), notebooks de saída (`*_output.ipynb`) e logs de execução (`logs/`) não devem ser mantidos nem versionados aqui.

```text
notebooks/
├── README.md
├── 01_urna_logs_poc/
│   ├── 01_urna_logs_poc.ipynb
│   └── kernel-metadata.json
├── 02_ingest_electoral_tables/
│   ├── 02_ingest_electoral_tables.ipynb
│   └── kernel-metadata.json
├── 03_ingest_urna_logs_batch/
│   ├── 03_ingest_urna_logs_batch.ipynb
│   └── kernel-metadata.json
├── 04_dataset_integrity_and_release/
│   ├── 04_dataset_integrity_and_release.ipynb
│   └── kernel-metadata.json
├── 05_silver_fact_urna_event/
│   ├── 05_silver_fact_urna_event.ipynb
│   └── kernel-metadata.json
├── 06_silver_fact_voting_session/
│   ├── 06_silver_fact_voting_session.ipynb
│   └── kernel-metadata.json
├── 07_silver_dimensions/
│   ├── 07_silver_dimensions.ipynb
│   └── kernel-metadata.json
├── 08_gold_analytics/
│   ├── 08_gold_analytics.ipynb
│   └── kernel-metadata.json
└── 09_release_pipeline/
    ├── 09_release_pipeline.ipynb
    └── kernel-metadata.json
```

| Fase | Notebook | Camada | Responsabilidade |
|---|---|---|---|
| 0 | `01_urna_logs_poc` | PoC | Extração parcial via HTTP Range de `.jez` e parsing 1:1 em memória (`TSE_PUBLISH_POC=1` opcional) |
| 1 | `02_ingest_electoral_tables` | Bronze | Ingestão 1:1 das 17 tabelas tabulares com verificação `.sha512` e streaming de CSVs grandes |
| 2 | `03_ingest_urna_logs_batch` | Bronze | Ingestão paralela em batch de 100% dos logs de urna (`fact_urna_logs`, zero PII, sharding `<= 128 MB`) |
| 3 | `04_dataset_integrity_and_release` | Bronze | Auditoria física, referencial (DuckDB), privacidade e geração de `dataset-metadata.json` |
| 4 | `05_silver_fact_urna_event` | Silver | Classificação semântica allowlist dos logs em `fact_urna_event` mantendo sharding multi-part |
| 5 | `06_silver_fact_voting_session` | Silver | Reconstrução de sessões eleitorais (`fact_voting_session`) via máquina de estados v1.0.0 |
| 6 | `07_silver_dimensions` | Silver | Materialização das dimensões conformadas `dim_election`, `dim_cargo` e `dim_partido` |
| 7 | `08_gold_analytics` | Gold | Produtos analíticos `gold_event_distribution`, `gold_session_completeness` e `gold_biometric_attempts` |
| 8 | `09_release_pipeline` | Release | Auditoria multi-camada (26 tabelas), geração do `manifest.json` final com SHA-256 e upload |

---

## 2. Contrato Unificado de Caminhos e Variáveis de Ambiente

Todos os 9 notebooks compartilham o mesmo contrato de bootstrap e persistência via **KaggleHub SDK (`kagglehub>=0.4.0`)**:

| Variável | Padrão | Descrição |
|---|---|---|
| `KAGGLE_DATASET_HANDLE` | `daniellmattos/tse-eleicoes-dados-v2` | Dataset alvo no Kaggle para download (`ALLOW_COLD_START=False` em `04`–`09`) e publicação incremental |
| `DATASET_PATH` | `/content/drive/MyDrive/tse-eleicoes-dados/data` | Diretório persistente do dataset em Parquet ZSTD e `_checkpoints/manifest.json` |
| `STAGING_PATH` | `/tmp/tse-eleicoes-staging` | Staging efêmero em SSD local da VM (`.zip`, `.jez` e Parquet temporários antes da cópia ao Drive) |

### Parâmetros de Controle e Paralelismo (Notebooks `01`–`04`)

- `TSE_TARGET_UFS`: Lista separada por vírgula (ex.: `AC,AL,AM`). Padrão: todas as 28 UFs (`AC`..`TO`, `ZZ`).
- `TSE_TARGET_TURNS`: Lista separada por vírgula (ex.: `1,2`). Padrão: `1,2`.
- `TSE_MAX_JEZ_FILES` *(NB03)*: Limite de arquivos `.jez` por partição para amostragem/testes; vazio/`None` ingere 100% das urnas.
- `TSE_MAX_PARQUET_PART_BYTES` *(NB03)*: Tamanho máximo de cada arquivo `part-N.parquet` em bytes (padrão `134217728` = 128 MB).
- `TSE_PREFETCH_DOWNLOADERS` *(NB03)*: Workers de prefetch de `.zip` estaduais em background (padrão `2` no notebook, `3` em `scripts/run_colab_cli.sh`).
- `TSE_PROC_WORKERS` *(NB03)*: Processos paralelos para descompressão e parsing de `.jez` (padrão `min(16, cpu_count)` no notebook, `8` em `scripts/run_colab_cli.sh`).
- `TSE_CHUNK_JEZ_SIZE` *(NB03)*: Quantidade de `.jez` processados por lote em memória (padrão `250` no notebook, `500` em `scripts/run_colab_cli.sh`).
- `TSE_MANIFEST_NAME` *(NB03)*: Nome do manifesto em `_checkpoints/` (padrão `manifest.json`; ex.: `manifest_w1.json` em workers sharded).
- `TSE_SKIP_KAGGLE_RELEASE` *(NB03)*: Defina `1`/`true` em workers sharded para pular `kagglehub.dataset_upload()` até a consolidação final.
- `TSE_AUDIT_UFS` *(NB04)*: Filtra UFs auditadas no NB04.
- `TSE_PUBLISH_POC` *(NB01)*: Defina `1` para permitir que a PoC publique no KaggleHub (desabilitado por padrão).

---

## 3. Segurança e Gestão de Credenciais (Repositório Público)

> **Regra estrita:** Nunca insira `KAGGLE_API_TOKEN`, `kaggle.json`, `.env` ou segredos pessoais em notebooks, scripts bash ou commits Git.

Os notebooks resolvem `KAGGLE_API_TOKEN` automaticamente na seguinte ordem:
1. Variável de ambiente `KAGGLE_API_TOKEN` (injetada pelo `scripts/run_colab_cli.sh` ou pelo shell).
2. Cache local restrito na VM Colab: `/root/.config/kaggle/colab_token.env` (`chmod 600`).
3. **Colab Secrets** (`google.colab.userdata.get("KAGGLE_API_TOKEN")`) quando executado pela interface web do Google Colab.
4. Prompt interativo `kagglehub.login()` apenas quando há TTY e nenhuma credencial no ambiente.

O arquivo `.gitignore` bloqueia `.env`, `*.env`, `*.env.*`, `colab_token.env`, `kaggle.json`, `logs/`, `*_output.ipynb` e diretórios `data/`.

---

## 4. Execução Reproduzível

### 4.1 Via Google Colab Web UI

1. Abra o notebook desejado no Google Colab.
2. Cadastre o segredo `KAGGLE_API_TOKEN` no painel **Secrets** (ícone de chave na barra lateral esquerda) e ative o acesso ao notebook.
3. Execute **Runtime -> Run all**. A célula de bootstrap monta automaticamente `/content/drive`, injeta `KAGGLE_API_TOKEN` e hidrata `DATASET_PATH` caso esteja vazio.

### 4.2 Via Google Colab CLI (`scripts/run_colab_cli.sh`) — Recomendado

Pré-requisitos na máquina local:
```bash
uv tool install google-colab-cli
export KAGGLE_API_TOKEN="<seu-token-kaggle>"   # ou salve via prompt seguro em ~/.config/kaggle/colab_token.env (chmod 600)
```

Execute qualquer notebook da pipeline por ID (`01`..`09`) ou caminho:
```bash
# Fase 1: 17 tabelas eleitorais (Bronze)
./scripts/run_colab_cli.sh 02 tse-nb02

# Fase 2: 100% dos logs de urna (Bronze) em sessão única persistente
./scripts/run_colab_cli.sh 03 tse-nb03

# Filtrando UFs/turnos específicos (ex.: reprocessamento pontual)
./scripts/run_colab_cli.sh 03 tse-nb03 "AC,AL,AM" "1,2"

# Fases seguintes (04 a 09)
./scripts/run_colab_cli.sh 04 tse-nb04
./scripts/run_colab_cli.sh 05 tse-nb05
./scripts/run_colab_cli.sh 06 tse-nb06
./scripts/run_colab_cli.sh 07 tse-nb07
./scripts/run_colab_cli.sh 08 tse-nb08
./scripts/run_colab_cli.sh 09 tse-nb09
```

O script `scripts/run_colab_cli.sh` cria ou reutiliza a sessão Colab, monta o Google Drive uma única vez (`colab drivemount`), limpa `/tmp/tse-eleicoes-staging` e eventuais `*.parquet.tmp` órfãos, executa com `--timeout 86400` e retoma automaticamente em caso de queda usando `colab restart-kernel` (preservando o mount do Drive).

### 4.3 Execução Paralela Multi-Sessão (Sharded) no Colab CLI

Para acelerar a ingestão completa da Fase 2 (`NB03`) dividindo UFs entre sessões Colab independentes sem disputa de `manifest.json`:

```bash
# Terminal 1 (Worker 1)
TSE_MANIFEST_NAME=manifest_w1.json TSE_SKIP_KAGGLE_RELEASE=1 \
  ./scripts/run_colab_cli.sh 03 tse-nb03-w1 "SP,MG,RJ,BA,PR,RS" "1,2"

# Terminal 2 (Worker 2)
TSE_MANIFEST_NAME=manifest_w2.json TSE_SKIP_KAGGLE_RELEASE=1 \
  ./scripts/run_colab_cli.sh 03 tse-nb03-w2 "PE,CE,PA,SC,GO,MA,PB,RN,ES,PI,AL,MT,MS,DF,SE,RO,TO,AC,AP,RR,AM,ZZ" "1,2"
```

Após todos os workers concluírem, consolide os manifestos `manifest_w*.json` no `manifest.json` oficial e publique uma única versão no KaggleHub:
```bash
python3 scripts/merge_sharded_manifests_and_release.py
```

### 4.4 Via Kaggle CLI

```bash
cd notebooks/05_silver_fact_urna_event
kaggle kernels push -p .
kaggle kernels status daniellmattos/tse-analysis-05-silver-fact-urna-event
```

> **Nunca use** `kaggle datasets version ... --dir-mode zip`: isso compacta pastas Hive e quebra a descoberta particionada.

---

## 5. Lições Aprendidas de Engenharia

Documentadas formalmente em [../AGENTS.md](../AGENTS.md) e aplicadas em todos os notebooks:

1. **Sharding Rolling `<= 128 MB` com Invariante de Fronteira de Urna (`.jez`):** Partições volumosas (ex.: SP/MG/RJ) são divididas em `part-0.parquet`, `part-1.parquet`, ... abaixo de 128 MB (`TSE_MAX_PARQUET_PART_BYTES`) sem jamais partir um arquivo `.jez` ao meio, preservando a descoberta nativa por `*.parquet` no DuckDB, Polars e Spark e permitindo resharding local in-place sem tráfego de rede.
2. **Segurança no Google Drive FUSE (`/content/drive/MyDrive`):** Nunca sobrescreva ou use `os.replace()` diretamente sobre arquivos no Drive FUSE (handles residuais podem preservar inodes antigos e duplicar dados). Todos os notebooks gravam Parquets primeiro em staging local (`/tmp/...`), removem completamente os arquivos antigos na pasta de destino do Drive, copiam os arquivos finalizados e validam o tamanho em bytes.
3. **Atualização Atômica e Lockada do `manifest.json`:** `NB02` e `NB03` protegem `_checkpoints/manifest.json` com `threading.Lock` + `fcntl.flock` e escrita temporária `.json.tmp` antes do rename atômico.
4. **Preservação do Mount do Drive no `colab` CLI:** Nunca execute `colab stop -s <sessão>` para interromper ou reiniciar uma execução — isso libera a VM e derruba o mount interativo do Drive. Use sempre `colab restart-kernel -s <sessão>`.
5. **Compatibilidade com DuckDB 2.0+:** Não chame `.fetchone()` em objetos `DuckDBPyRelation`; execute consultas via `con.execute(query).fetchone()` ou converta para Arrow/Polars.
6. **Streaming de Memória Limitada:** CSVs estaduais gigantes (ex.: `bweb` / `votacao_secao` em SP) são lidos em streaming de 16 MB (`pyarrow.csv.open_csv` -> `ParquetWriter`), `.jez` são extraídos 100% em memória (`io.BytesIO`) com prefetch de downloads, e a camada Silver (`05_silver_fact_urna_event`) processa e preserva shards 1:1 sem carregar toda a UF na RAM.
