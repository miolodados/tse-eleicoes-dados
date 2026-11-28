# Dicionário de Dados — Schemas 1:1 das Fontes Reais 2026 (`data_dictionary`)

Este documento descreve os contratos de dados das tabelas do projeto `tse-analysis` para as **Eleições 2026**.

Todas as tabelas refletem **fielmente o formato original da fonte (1:1)** em **Parquet ZSTD particionado** (`election_year=2026/turn={1,2}/uf={UF}`), com validação de integridade física (hashes `.sha512`, contagem exata de linhas e tipos nativos):
- **Logs da Urna (`fact_urna_logs`):** Preserva a estrutura tabulada original de `logd.dat` mantendo `message_text` (com números de identificação de mesário/eleitor de 5+ dígitos mascarados como `<ID_REDACTED>`) e omitindo o campo final de código de autenticação hexadecimal (`mac_code`), o que evita ~35 GB de entropia aleatória incompressível e permite altíssima compressão por dicionário em Parquet ZSTD para publicação no Kaggle.
- **Tabelas Tabulares e Dimensões:** Mantêm 100% dos arquivos e colunas originais dos CSVs de 2026 do TSE, aproveitando o *dictionary encoding* nativo do Parquet ZSTD para comprimir colunas descritivas repetidas sem perda de fidelidade à fonte.
- **Comparecimento e Abstenção (`fact_comparecimento_abstencao`):** Incorporada integralmente ao dataset público no schema 1:1 (`redistribution_allowed: true`), considerando a ausência de `license_id` no metadado CKAN como omissão formal do portal para dados públicos abertos do TSE.

> **Status da publicação v2 (`daniellmattos/tse-eleicoes-dados-v2`, Público, `CC-BY-SA-4.0`):** todas as **28 tabelas** (18 Bronze + 5 Silver + 5 Gold) estão implementadas, executadas em cobertura nacional integral (28 UFs incluindo `ZZ`, 1º e 2º turnos de 2026) e auditadas via `09_release_pipeline.ipynb` (`manifest.json` com SHA-256). O ledger de status publicado e o mapeamento de tabelas para o consumidor **TS-Zé (`miolodados/tsze#8`)** constam na Seção 7 abaixo.

---

## 1. Chaves Naturais de Contexto (`SECTION_TURN_KEY_SCHEMA`)

| Coluna Lógica | Coluna Original TSE | Tipo PyArrow / Polars | Descrição |
|---|---|---|---|
| `election_year` | `ANO_ELEICAO` / `AA_ELEICAO` | `int16` / `Int16` (`Int32` em `fact_urna_logs`) | Ano da eleição (`2026`). |
| `turn` | `NR_TURNO` | `int8` / `Int8` | Número do turno (`1` ou `2`). |
| `uf` | `SG_UF` | `string` / `String` | Sigla da Unidade Federativa (`AC`..`TO`, `BR`, `ZZ`). |
| `municipio_code` | `CD_MUNICIPIO` | `int32` / `Int32` | Código TSE do município (5 dígitos). |
| `zone_num` | `NR_ZONA` | `int16` / `Int16` (`Int32` em `fact_urna_logs`) | Número da zona eleitoral (4 dígitos). |
| `section_num` | `NR_SECAO` | `int32` / `Int32` | Número da seção eleitoral (4 dígitos). |

---

## 2. Tabelas Fato, Pontes e Dimensões (Schemas 1:1 Inferidos dos Arquivos Reais de 2026)

### 2.1. `fact_urna_logs`
- **Fonte Primária:** `totalizacao_arquivos_transmitidos` (`https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes2026/arqurnatot/bu_imgbu_logjez_rdv_vscmr_2026_{turn}t_{uf}.zip` $\rightarrow$ arquivos `o<pleito:5><uf:2><mun:5><zona:4><secao:4>-log.jez` $\rightarrow$ `logd.dat` e segmentos rotacionados) e `ecossistema_urna_log_docs` (`https://www.tse.jus.br/eleicoes/eleicoes-2026-content/arquivos/log-do-ecossistema/@@display-file/file/log-do-ecossistema.pdf`).
- **Grão:** Linha física do arquivo de log da urna (`urna_log_event`), ordenada por `file_seq`.
- **Volumetria Integral (2026):** `998.354` arquivos `.jez`, `7.976.857.557` linhas em 56 partições (28 UFs × 2 turnos, `35,58 GB` Bronze total).
- **Schema físico em Parquet (`15 colunas`):**
  - `election_year` (`Int32`): Ano da eleição (`2026`).
  - `turn` (`Int8`): Turno (`1` ou `2`).
  - `uf` (`String`): UF (`AC`..`TO`, `ZZ`).
  - `cd_pleito` (`Int64`): Código do pleito extraído do nome do `.jez` (`3220` no 1º turno e `3221` no 2º turno de 2026).
  - `cd_municipio` (`Int32`): Código TSE do município extraído do nome do `.jez` (ex.: `1015`).
  - `nr_zona` (`Int32`): Zona eleitoral extraída do nome do `.jez` (ex.: `2`).
  - `nr_secao` (`Int32`): Seção eleitoral extraída do nome do `.jez` (ex.: `88`).
  - `source_jez_file` (`String`): Nome do arquivo `.jez` de origem dentro do pacote da UF.
  - `source_log_member` (`String`): Membro interno do `.jez` (`logd.dat`, `logd.dat.1`, etc.).
  - `file_seq` (`Int64`): Número sequencial monotônico da linha dentro do arquivo `.jez` (`1..N`).
  - `raw_timestamp` (`String`): Data e hora registradas no 1º campo tabulado de `logd.dat` (`DD/MM/YYYY HH:MM:SS`, convertidas para `Datetime("us")` na Silver `fact_urna_event`).
  - `log_level` (`String`): Nível de severidade original do 2º campo tabulado (`INFO`, `ALERTA`, `ERRO`, `DEBUG`).
  - `urna_serial` (`String`): Número de série da urna do 3º campo tabulado (ex.: `"02263093"`).
  - `application` (`String`): Aplicativo emissor do 4º campo tabulado (`LOGD`, `SCUE`, `VOTA`, `GAP`, `RED`, etc.).
  - `message_text` (`String`): Mensagem original do 5º campo tabulado de `logd.dat` (com números de 5+ dígitos mascarados como `<ID_REDACTED>` para proteção de privacidade e máxima compressão por dicionário no Parquet ZSTD; o 6º campo `mac_code` é omitido na persistência para eliminar entropia hexadecimal aleatória).

### 2.2. `fact_boletim_urna`
- **Fonte Primária:** `resultados_boletim_urna` (`buweb/bweb_{turn}t_{uf}_<DDMMYYYYHHMM>.zip`, validado contra o respectivo `.sha512`).
- **Grão:** `bu_section_cargo_vote` (Seção $\times$ Turno $\times$ Cargo/Pergunta $\times$ Votável).
- **Schema 1:1 (45 colunas nativas do CSV de 2026):**
  `DT_GERACAO`, `HH_GERACAO`, `ANO_ELEICAO`, `CD_TIPO_ELEICAO`, `NM_TIPO_ELEICAO`, `CD_PLEITO`, `DT_PLEITO`, `NR_TURNO`, `CD_ELEICAO`, `DS_ELEICAO`, `SG_UF`, `CD_MUNICIPIO`, `NM_MUNICIPIO`, `NR_ZONA`, `NR_SECAO`, `NR_LOCAL_VOTACAO`, `CD_CARGO_PERGUNTA`, `DS_CARGO_PERGUNTA`, `NR_PARTIDO`, `SG_PARTIDO`, `NM_PARTIDO`, `DT_BU_RECEBIDO`, `QT_APTOS`, `QT_COMPARECIMENTO`, `QT_ABSTENCOES`, `CD_TIPO_URNA`, `DS_TIPO_URNA`, `CD_TIPO_VOTAVEL`, `DS_TIPO_VOTAVEL`, `NR_VOTAVEL`, `NM_VOTAVEL`, `QT_VOTOS`, `NR_URNA_EFETIVADA`, `CD_CARGA_1_URNA_EFETIVADA`, `CD_CARGA_2_URNA_EFETIVADA`, `CD_FLASHCARD_URNA_EFETIVADA`, `DT_CARGA_URNA_EFETIVADA`, `DS_CARGO_PERGUNTA_SECAO`, `DS_SECOES_AGREGADAS`, `DT_ABERTURA`, `DT_ENCERRAMENTO`, `QT_ELEI_BIOM_SEM_HABILITACAO`, `DT_EMISSAO_BU`, `NR_JUNTA_APURADORA`, `NR_TURMA_APURADORA`.

### 2.3. `fact_votacao_secao`, `fact_detalhe_votacao_secao`, `dim_candidato_munzona` e `dim_partido_munzona`
- **Fonte Primária:** `resultados_apuracao_secao` (`resultados-2026`).
- **Tabelas 1:1 dos CSVs de 2026:**
  1. `fact_votacao_secao` (`votacao_secao_2026_{uf}.zip`, 26 colunas): `DT_GERACAO`, `HH_GERACAO`, `ANO_ELEICAO`, `CD_TIPO_ELEICAO`, `NM_TIPO_ELEICAO`, `NR_TURNO`, `CD_ELEICAO`, `DS_ELEICAO`, `DT_ELEICAO`, `TP_ABRANGENCIA`, `SG_UF`, `SG_UE`, `NM_UE`, `CD_MUNICIPIO`, `NM_MUNICIPIO`, `NR_ZONA`, `NR_SECAO`, `CD_CARGO`, `DS_CARGO`, `NR_VOTAVEL`, `NM_VOTAVEL`, `QT_VOTOS`, `NR_LOCAL_VOTACAO`, `SQ_CANDIDATO`, `NM_LOCAL_VOTACAO`, `DS_LOCAL_VOTACAO_ENDERECO`.
  2. `fact_detalhe_votacao_secao` (`detalhe_votacao_secao_2026.zip`, 37 colunas): `DT_GERACAO`, `HH_GERACAO`, `ANO_ELEICAO`, `CD_TIPO_ELEICAO`, `NM_TIPO_ELEICAO`, `NR_TURNO`, `CD_ELEICAO`, `DS_ELEICAO`, `DT_ELEICAO`, `TP_ABRANGENCIA`, `SG_UF`, `SG_UE`, `NM_UE`, `CD_MUNICIPIO`, `NM_MUNICIPIO`, `NR_ZONA`, `NR_SECAO`, `CD_CARGO`, `DS_CARGO`, `QT_APTOS`, `QT_COMPARECIMENTO`, `QT_ABSTENCOES`, `QT_VOTOS_NOMINAIS`, `QT_VOTOS_BRANCOS`, `QT_VOTOS_NULOS`, `QT_VOTOS_LEGENDA`, `QT_VOTOS_ANULADOS_APU_SEP`, `NR_LOCAL_VOTACAO`, `NM_LOCAL_VOTACAO`, `DS_LOCAL_VOTACAO_ENDERECO`, `DT_RECEBIMENTO_BU_HOR_TSE`, `DT_PRIM_TOT_PARCIAL_HOR_TSE`, `DS_ORIGEM_VOTO`, `ST_SECAO_INSTALADA`, `ST_SECAO_ANULADA`, `CD_MODELO_URNA`, `DS_MODELO_URNA`.
  3. `fact_detalhe_votacao_munzona` (`detalhe_votacao_munzona_2026.zip`, 47 colunas).
  4. `dim_candidato_munzona` (`votacao_candidato_munzona_2026.zip`, 50 colunas) e `dim_partido_munzona` (`votacao_partido_munzona_2026.zip`, 38 colunas).

### 2.4. `fact_eleitorado_perfil_secao`, `dim_local_votacao_secao`, `fact_eleitorado_deficiencia` e `fact_transferencia_temporaria_secao`
- **Fonte Primária:** `eleitorado_perfil` (`eleitorado-2026`).
- **Tabelas 1:1 dos CSVs de 2026:**
  1. `fact_eleitorado_perfil_secao` (`perfil_eleitor_secao_2026_{uf}.zip`, 30 colunas): `DT_GERACAO`, `HH_GERACAO`, `AA_ELEICAO`, `SG_UF`, `CD_MUNICIPIO`, `NM_MUNICIPIO`, `NR_ZONA`, `NR_SECAO`, `NR_LOCAL_VOTACAO`, `NM_LOCAL_VOTACAO`, `CD_GENERO`, `DS_GENERO`, `CD_ESTADO_CIVIL`, `DS_ESTADO_CIVIL`, `CD_FAIXA_ETARIA`, `DS_FAIXA_ETARIA`, `CD_GRAU_ESCOLARIDADE`, `DS_GRAU_ESCOLARIDADE`, `CD_RACA_COR`, `DS_RACA_COR`, `CD_IDENTIDADE_GENERO`, `DS_IDENTIDADE_GENERO`, `CD_QUILOMBOLA`, `DS_QUILOMBOLA`, `CD_INTERPRETE_LIBRAS`, `DS_INTERPRETE_LIBRAS`, `QT_ELEITORES`, `QT_ELEITORES_BIOMETRIA`, `QT_ELEITORES_DEFICIENCIA`, `QT_ELEITORES_NOME_SOCIAL`.
  2. `fact_eleitorado_perfil_munzona` (`perfil_eleitorado_2026.zip`, 27 colunas).
  3. `dim_local_votacao_secao` (`eleitorado_local_votacao_2026.zip`, 41 colunas): cadastro geográfico completo de seções e locais de votação (`DS_ENDERECO`, `NM_BAIRRO`, `NR_CEP`, `NR_LATITUDE`, `NR_LONGITUDE`, `CD_TIPO_SECAO_AGREGADA`, `NR_SECAO_PRINCIPAL`, `QT_ELEITOR_SECAO`, etc.).
  4. `fact_eleitorado_deficiencia` (`perfil_eleitor_deficiencia_2026.zip`, 30 colunas) e `fact_transferencia_temporaria_secao` (`transf_temporaria_secao_2026.zip`, 17 colunas) / `fact_transferencia_temporaria_mun` (`transferencia_temporaria_2026.zip`, 13 colunas).

### 2.5. `fact_comparecimento_abstencao`, `fact_comparecimento_abstencao_pcd` e `fact_comparecimento_abstencao_tte`
- **Fonte Primária:** `comparecimento_abstencao` (`comparecimento-e-abstencao-2026` — Sistema ELO, incorporado ao dataset público com schema 1:1).
- **Tabelas 1:1 dos CSVs de 2026:**
  1. `fact_comparecimento_abstencao` (`perfil_comparecimento_abstencao_2026.zip`, 45 colunas): `DT_GERACAO`, `HH_GERACAO`, `ANO_ELEICAO`, `NR_TURNO`, `SG_UF`, `CD_MUNICIPIO`, `NM_MUNICIPIO`, `NR_ZONA`, `CD_GENERO`, `DS_GENERO`, `CD_ESTADO_CIVIL`, `DS_ESTADO_CIVIL`, `CD_FAIXA_ETARIA`, `DS_FAIXA_ETARIA`, `CD_GRAU_INSTRUCAO`, `DS_GRAU_INSTRUCAO`, `CD_COR_RACA`, `DS_COR_RACA`, `CD_QUILOMBOLA`, `DS_QUILOMBOLA`, `CD_INTERPRETE_LIBRAS`, `DS_INTERPRETE_LIBRAS`, `CD_IDENTIDADE_GENERO`, `DS_IDENTIDADE_GENERO`, `CD_IDIOMA_INDIGENA`, `DS_IDIOMA_INDIGENA`, `CD_GRUPO_INDIGENA`, `DS_GRUPO_INDIGENA`, `CD_IDIOMA_QUILOMBOLA`, `DS_IDIOMA_QUILOMBOLA`, `QT_APTOS`, `QT_COMPARECIMENTO`, `QT_ABSTENCAO`, `QT_COMPARECIMENTO_DEFICIENCIA`, `QT_ABSTENCAO_DEFICIENCIA`, `QT_COMPARECIMENTO_TTE`, `QT_ABSTENCAO_TTE`, `QT_COMPAREC_FACULTATIVO`, `QT_ABST_FACULTATIVO`, `QT_COMPAREC_OBRIGATORIO`, `QT_ABST_OBRIGATORIO`, `QT_COMPAREC_DEFIC_FACULTATIVO`, `QT_ABST_DEFIC_FACULTATIVO`, `QT_COMPAREC_DEFIC_OBRIGATORIO`, `QT_ABST_DEFIC_OBRIGATORIO`.
  2. `fact_comparecimento_abstencao_pcd` (`perfil_comparecimento_abstencao_eleitor_deficiente_2026.zip`, 20 colunas).
  3. `fact_comparecimento_abstencao_tte` (`perfil_comparecimento_abstencao_eleitor_tte_2026.zip`, 25 colunas).

### 2.6. `bridge_correspondencia_esperada` e `bridge_correspondencia_efetivada`
- **Fonte Primária:** `correspondencias_esperadas_efetivadas` (1º e 2º turnos, validado contra `.sha512`).
- **Tabelas 1:1 dos CSVs de 2026:**
  1. `bridge_correspondencia_esperada` (`correspesp/CESP_{turn}t_{uf}_<DDMMYYYYHHMM>.zip`, 21 colunas): `DT_GERACAO`, `HH_GERACAO`, `AA_ELEICAO`, `CD_PLEITO`, `SG_UF`, `CD_MUNICIPIO`, `NM_MUNICIPIO`, `NR_ZONA`, `NR_SECAO`, `NR_LOCAL_VOTACAO`, `NR_URNA_ESPERADA`, `CD_CARGA_URNA_ESPERADA`, `CD_FLASHCARD_URNA_ESPERADA`, `DT_CARGA_URNA_ESPERADA`, `ST_CORRESP_ALTERADA`, `NM_MAQUINA_GERACAO_MIDIA`, `NR_SRI_TPM_GERACAO_MIDIA`, `NR_SRI_INSTAL_GERACAO_MIDIA`, `NM_MAQUINA_TRANSM_CORRESP`, `NR_SRI_TPM_TRANSM_CORRESP`, `NR_SRI_INSTAL_TRANSM_CORRESP`.
  2. `bridge_correspondencia_efetivada` (`correspefet/CEFT_{turn}t_{uf}_<DDMMYYYYHHMM>.zip`, 36 colunas): `DT_GERACAO` .. `AA_ELEICAO`, `DT_PLEITO`, `CD_PLEITO`, `SG_UF`, `CD_MUNICIPIO`, `NM_MUNICIPIO`, `NR_ZONA`, `NR_SECAO`, `NR_LOCAL_VOTACAO`, `SQ_CORRESPONDENCIA_ESPERADA`, `NR_URNA_ESPERADA`, `CD_CARGA_URNA_ESPERADA`, `CD_CARGA_1_URNA_ESPERADA`, `CD_CARGA_2_URNA_ESPERADA`, `CD_FLASHCARD_URNA_ESPERADA`, `DT_CARGA_URNA_ESPERADA`, `DT_RECBTO_URNA_ESPERADA`, `SQ_CORRESPONDENCIA_EFETIVADA`, `NR_URNA_EFETIVADA`, `CD_CARGA_URNA_EFETIVADA`, `CD_CARGA_1_URNA_EFETIVADA`, `CD_CARGA_2_URNA_EFETIVADA`, `CD_FLASHCARD_URNA_EFETIVADA`, `DT_CARGA_URNA_EFETIVADA`, `DT_RECBTO_URNA_EFETIVADA`, `CD_ORIGEM_VOTO`, `DS_ORIGEM_VOTO`, `TP_DIVERGENCIA`, etc.

---

## 3. Política de Armazenamento Parquet e Particionamento

- **Formato Físico:** Apache Parquet com compressão **ZSTD** (nível 5).
- **Particionamento Permitido (`ALLOWED_PARTITION_KEYS`):** Exclusivamente `("election_year", "turn", "uf")` ou `("turn", "uf")`.
- **Particionamento Proibido (`FORBIDDEN_PARTITION_KEYS`):** É proibido particionar por `section_num`, `urna_serial`, `municipio_code` ou `raw_timestamp`.

---

## 4. Camada Silver (Tabelas Fato Interpretadas e Dimensões Conformadas)

### 4.1. `fact_urna_event`
- **Fonte Primária:** `fact_urna_logs` (Bronze) interpretado deterministicamente contra a allowlist de `configs/event_catalog.yml` via Notebook `05_silver_fact_urna_event`. Nas partições `turn=2`, o prefixo de carryover do 1º turno em urnas reutilizadas é removido de forma determinística e imune a *clock drift* (via `turn_boundary_patterns` em `file_seq`, histórico de `urna_serial` do 1º turno e fallback de data apenas em timestamps válidos sem drift), mantendo a Bronze 1:1 intacta.
- **Grão:** Uma linha por evento interpretado (`urna_event`) do respectivo turno.
- **Schema (`17 colunas`):**
  - `election_year` (`Int16`): Ano da eleição.
  - `turn` (`Int8`): Turno (1 ou 2).
  - `uf` (`String`): Unidade Federativa.
  - `municipio_code` (`Int32`): Código TSE do município.
  - `zone_num` (`Int16`): Zona eleitoral.
  - `section_num` (`Int32`): Seção eleitoral.
  - `urna_serial` (`String`): Número de série da urna.
  - `file_seq` (`Int64`): Sequência monotônica do evento.
  - `raw_timestamp` (`Datetime("us")`): Timestamp original da linha de log.
  - `is_clock_drift` (`Boolean`): Indicador de *clock drift* da BIOS (timestamp fora da janela eleitoral esperada, ex.: `01/01/1970`); registros com `is_clock_drift = True` são preservados na Silver.
  - `has_timestamp_regression` (`Boolean`): Indicador de regressão temporal.
  - `application` (`String`): Aplicativo de origem (`VOTA`, `GAP`, etc.).
  - `log_level` (`String`): Nível de severidade original.
  - `event_code` (`String`): Família canônica do catálogo (ex.: `SECTION_OPENED`, `VOTER_ENABLED`, `BIOMETRIC_READ_ATTEMPT`, `UNKNOWN_EVENT`).
  - `biometric_attempt_num` (`Int8`): Número da tentativa biométrica (1..127) quando aplicável.
  - `classification_status` (`String`): `"CLASSIFIED"` ou `"UNKNOWN"`.
  - `parser_version` (`String`): Versão do parser e catálogo de eventos (`1.1.0`).

### 4.2. `fact_voting_session`
- **Fonte Primária:** `fact_urna_event` (Silver) reconstruído deterministicamente via máquina de estados v1.0.0 no Notebook `06_silver_fact_voting_session`.
- **Grão:** Uma linha por sessão operacional de urna/seção (`voting_session`).
- **Schema (`15 colunas`):**
  - `election_year` (`Int16`), `turn` (`Int8`), `uf` (`String`), `municipio_code` (`Int32`), `zone_num` (`Int16`), `section_num` (`Int32`), `urna_serial` (`String`).
  - `first_event_ts` (`Datetime`): Timestamp do primeiro evento observado da sessão.
  - `last_event_ts` (`Datetime`): Timestamp do último evento observado da sessão.
  - `final_state` (`String`): `"CLOSED"` (seção encerrada normalmente) ou `"INCOMPLETE"` (sem evento terminal).
  - `is_complete` (`Boolean`): `True` se `final_state == "CLOSED"`.
  - `has_contingency` (`Boolean`): Indicador de evento de contingência durante a sessão.
  - `voter_cycles` (`Int32`): Quantidade de ciclos completos de votação observados (`VOTER_VOTE_COMPUTED`).
  - `event_count` (`Int32`): Total de eventos contabilizados na sessão.
  - `state_machine_version` (`String`): Versão da máquina de estados (`1.0.0`).

### 4.3. Dimensões Conformadas Silver (Schemas Físicos em Parquet)
- **`dim_election` (`6 colunas`, `2 linhas`):** `election_year` (`Int16`), `turn` (`Int8`), `election_label` (`String`), `election_date_turn1` (`String`), `election_date_turn2` (`String`), `cd_pleito_turn1` (`Int32`).
- **`dim_cargo` (`2 colunas`, `6 linhas`):** `cd_cargo` (`Int32`), `ds_cargo` (`String`).
- **`dim_partido` (`3 colunas`, `30 linhas`):** `cd_partido` (`Int32`), `sg_partido` (`String`), `nm_partido` (`String`).

---

## 5. Camada Gold (Produtos Analíticos e Métricas Operacionais — 5 Tabelas)

Gerada no Notebook `08_gold_analytics` a partir das camadas Silver (`fact_urna_event`, `fact_voting_session`) e enriquecida com atributos oficiais das tabelas Bronze (`fact_boletim_urna` filtrado em `CD_CARGO_PERGUNTA = 1`, `dim_local_votacao_secao`, `fact_eleitorado_perfil_secao`, `bridge_correspondencia_efetivada`, `fact_detalhe_votacao_secao`):

### 5.1. `gold_event_distribution` (`gold/gold_event_distribution.parquet`)
- **Grão:** `(event_code, turn, uf)`.
- **Schema (`7 colunas`):** `event_code` (`String`), `turn` (`Int8`), `uf` (`String`), `event_count` (`Int64`), `total_uf_events` (`Int64`), `percentage` (`Float64`), `clock_drift_count` (`Int64`).

### 5.2. `gold_session_completeness` (`gold/gold_session_completeness.parquet`)
- **Grão:** `(turn, uf, final_state)`.
- **Schema (`13 colunas`):** `turn` (`Int8`), `uf` (`String`), `final_state` (`String`), `session_count` (`Int64`), `total_sessions_uf` (`Int64`), `state_share_pct` (`Float64`), `completion_rate` (`Float64`), `contingency_session_count` (`Int64`), `total_voter_cycles` (`Int64`), `avg_voter_cycles` (`Float64`), `min_voter_cycles` (`Int32`), `max_voter_cycles` (`Int32`), `total_events` (`Int64`).

### 5.3. `gold_biometric_attempts` (`gold/gold_biometric_attempts.parquet`)
- **Grão:** `(turn, uf, biometric_attempt_num)`.
- **Schema (`8 colunas`):** `turn` (`Int8`), `uf` (`String`), `biometric_attempt_num` (`Int8`), `attempt_count` (`Int64`), `total_attempts_uf` (`Int64`), `percentage` (`Float64`), `cumulative_count` (`Int64`), `cumulative_share` (`Float64`).

### 5.4. `gold_section_turn_summary` (`gold/gold_section_turn_summary.parquet`)
- **Grão:** `section_turn = (election_year, turn, uf, municipio_code, zone_num, section_num)` (~`998 mil` linhas, arquivo único `<= 128 MB`).
- **Propósito:** Fato analítico central projetado para consumo sub-100ms pelo **TS-Zé (`miolodados/tsze#8`)** em DuckDB no Kaggle Free Tier, evitando varreduras sobre os `4,95 bilhões` de eventos.
- **Colunas (`46 colunas`):**
  - *Chaves e geografia:* `election_year` (`Int16`), `turn` (`Int8`), `uf` (`String`), `municipio_code` (`Int32`), `municipio_name` (`String`), `zone_num` (`Int16`), `section_num` (`Int32`), `local_votacao_num` (`Int32`), `local_votacao_name` (`String`), `latitude` (`Float64`), `longitude` (`Float64`).
  - *Métricas operacionais e biométricas (Silver):* `urna_count` (`Int32`), `session_final_state` (`String`), `all_sessions_complete` (`Boolean`), `has_contingency` (`Boolean`), `has_clock_drift` (`Boolean`), `has_timestamp_regression` (`Boolean`), `first_event_ts` (`Datetime("us")`), `last_event_ts` (`Datetime("us")`), `observed_duration_sec` (`Int64` — nulo quando `has_clock_drift = True`), `total_events` (`Int64`), `classified_events` (`Int64`), `unknown_events` (`Int64`), `voter_enabled_events` (`Int64`), `voter_cycles` (`Int64`), `biometric_attempt_events` (`Int64`), `biometric_recognized_events` (`Int64`), `biometric_not_recognized_events` (`Int64`), `no_biometric_registered_events` (`Int64`), `manual_enable_events` (`Int64`), `inactivity_timeout_events` (`Int64`), `invalid_key_events` (`Int64`), `biometric_recognition_rate_pct` (`Float64`), `manual_enable_rate_pct` (`Float64`).
  - *Comparecimento oficial e hardware (Bronze `fact_boletim_urna` `CD_CARGO_PERGUNTA=1` + `fact_detalhe_votacao_secao`):* `qt_aptos` (`Int64`), `qt_comparecimento` (`Int64`), `qt_abstencoes` (`Int64`), `qt_elei_biom_sem_habilitacao` (`Int64`), `turnout_rate_pct` (`Float64`), `urna_model_desc` (`String`).
  - *Perfil do eleitorado (Bronze `fact_eleitorado_perfil_secao`):* `qt_eleitores_perfil` (`Int64`), `qt_eleitores_biometria` (`Int64`), `biometria_coverage_pct` (`Float64`), `qt_eleitores_deficiencia` (`Int64`).
  - *Correspondência / troca de urna (Bronze `bridge_correspondencia_efetivada`):* `corresp_urna_swapped` (`Boolean`), `corresp_divergence_type` (`String`).

### 5.5. `gold_municipality_turn_summary` (`gold/gold_municipality_turn_summary.parquet`)
- **Grão:** `(election_year, turn, uf, municipio_code)` (~`11 mil` linhas, `< 1 MB`).
- **Colunas (`27 colunas`):** `election_year`, `turn`, `uf`, `municipio_code`, `municipio_name`, `section_count`, `closed_section_count`, `urna_swapped_count`, `clock_drift_section_count`, `qt_aptos`, `qt_comparecimento`, `qt_abstencoes`, `turnout_rate_pct`, `voter_cycles`, `total_events`, `biometric_attempt_events`, `biometric_recognized_events`, `manual_enable_events`, `inactivity_timeout_events`, `invalid_key_events`, `biometric_recognition_rate_pct`, `manual_enable_rate_pct`, `avg_observed_duration_sec`, `qt_eleitores_perfil`, `qt_eleitores_biometria`, `biometria_coverage_pct`, `qt_eleitores_deficiencia`.

---

## 6. Matriz de Entidades e Relacionamentos

| Entidade Origem | Chaves Naturais | Dimensão Conformada | Cardinalidade | Observação |
|---|---|---|---|---|
| `fact_urna_logs` | `urna_serial`, `file_seq` | — (Bronze 1:1) | N:1 por urna | Registros tabulados de log (7,98B linhas) |
| `fact_urna_event` | `urna_serial`, `file_seq` | `fact_urna_logs` | 1:1 (T1) / deduplicada (T2) | Remove prefixo de carryover do 1º turno em `turn=2` (4,95B linhas) |
| `fact_voting_session` | `mun`, `zona`, `secao`, `urna` | `fact_urna_event` | 1:N eventos | Reconstrução por máquina de estados (998.355 sessões) |
| `gold_section_turn_summary` | `election_year`, `turn`, `uf`, `mun`, `zona`, `secao` | `dim_election`, `dim_local_votacao_secao` | 1:1 por `section_turn` | Fato central Gold para consumo TS-Zé |
| `gold_municipality_turn_summary` | `election_year`, `turn`, `uf`, `mun` | `dim_election` | N:1 seções | Agregação municipal de `gold_section_turn_summary` |
| `fact_votacao_secao` | `mun`, `zona`, `secao`, `cargo` | `dim_cargo`, `dim_partido` | N:1 | Votação nominal e legenda |
| `fact_eleitorado_perfil_secao` | `mun`, `zona`, `secao` | `dim_local_votacao_secao` | N:1 | Perfil sociodemográfico |

---

## 7. Ledger de Publicação (28 Tabelas) e Guia de Consumo para `miolodados/tsze`

### 7.1. Identidade Canônica no Kaggle
- **Dataset Handle:** `daniellmattos/tse-eleicoes-dados-v2` (Público, licença `CC-BY-SA-4.0`, release `v2` com `manifest.json` SHA-256).
- **Status das Camadas:** 18 Bronze (`published`, 100% integral), 5 Silver (`published`, 7/7 assertions aprovadas), 5 Gold (`published`, reconciliadas contra Silver e Bronze).

### 7.2. Mapeamento de Nomes (Rascunho `tsze#8` $\rightarrow$ Tabela Publicada)

| Referência em `tsze#8` / Discussões | Tabela Publicada no Dataset v2 | Caminho Relativo no Kaggle |
|---|---|---|
| Fato central `section_turn` | `gold_section_turn_summary` | `gold/gold_section_turn_summary.parquet` |
| Agregado municipal × turno | `gold_municipality_turn_summary` | `gold/gold_municipality_turn_summary.parquet` |
| Distribuição de eventos | `gold_event_distribution` | `gold/gold_event_distribution.parquet` |
| Completude de sessões | `gold_session_completeness` | `gold/gold_session_completeness.parquet` |
| Tentativas biométricas | `gold_biometric_attempts` | `gold/gold_biometric_attempts.parquet` |
| Eventos de urna classificados | `fact_urna_event` | `silver/fact_urna_event/election_year=2026/turn={1,2}/uf={UF}/*.parquet` |
| Sessões reconstruídas | `fact_voting_session` | `silver/fact_voting_session/election_year=2026/turn={1,2}/uf={UF}/*.parquet` |
| Dimensões conformadas | `dim_election`, `dim_cargo`, `dim_partido` | `silver/{tabela}/**/*.parquet` |
| Comparecimento oficial 100% T1+T2 | `fact_boletim_urna` (`CD_CARGO_PERGUNTA = 1`) | `fact_boletim_urna/election_year=2026/turn={1,2}/uf={UF}/*.parquet` |
| Modelo de hardware da urna | `fact_detalhe_votacao_secao` (`DS_MODELO_URNA`) | `fact_detalhe_votacao_secao/election_year=2026/turn={1,2}/uf={UF}/*.parquet` |

### 7.3. Recomendações de Performance DuckDB (Kaggle Free Tier)
1. **Gold-first:** Consulte `gold/gold_section_turn_summary.parquet` e `gold/gold_municipality_turn_summary.parquet` para 95%+ das perguntas do TS-Zé (latência típica `< 100ms`, `< 200 MB` de RAM).
2. **Partition pruning em Silver/Bronze:** Sempre filtre explicitamente por `turn` e `uf` (ou direcione o glob `turn=1/uf=SP/*.parquet`) ao consultar `silver/fact_urna_event` (`4,95B` linhas) ou `fact_urna_logs` (`7,98B` linhas).
3. **DuckDB 2.0:** Use `duckdb.connect(":memory:").execute(sql).fetchall()` / `.pl()` em vez de `.fetchone()` sobre objetos `Relation`.
4. **Duração de sessão e clock drift:** Utilize `observed_duration_sec` de `gold_section_turn_summary` (que já aplica guarda contra `has_clock_drift = True`, retornando `NULL` em urnas com relógio BIOS 1970).

