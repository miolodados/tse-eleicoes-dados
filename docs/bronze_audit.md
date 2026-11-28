# Auditoria Bronze e prontidão operacional do Kaggle

> **Atualização Operacional (Release v2 — Cobertura Nacional Integral Concluída):** Os achados abaixo preservam o registro histórico da auditoria estática inicial (pré-ingestão em lote). Desde então, todas as lacunas listadas foram fechadas e verificadas em produção:
> - **Cobertura Integral 100% (2026):** `998.354` arquivos `.jez` (`7.976.857.557` linhas Bronze em 56 partições — 28 UFs incluindo `ZZ`, 1º e 2º turnos, `35,58 GB`) + 17 tabelas eleitorais oficiais do TSE.
> - **Camada Silver (7/7 Assertions Aprovadas):** `fact_urna_event` (`4.954.541.320` linhas em 322 shards `<= 128 MB`, remoção exata de `3.022.316.237` linhas de carryover do 1º turno em `turn=2`), `fact_voting_session` (`998.355` sessões, 0 duplicatas), `dim_election` (2), `dim_cargo` (6), `dim_partido` (30, `0,00%` órfãos).
> - **Camada Gold Enriquecida (5 tabelas):** `gold_event_distribution`, `gold_session_completeness`, `gold_biometric_attempts`, `gold_section_turn_summary` e `gold_municipality_turn_summary`.
> - **Publicação Canônica:** `daniellmattos/tse-eleicoes-dados-v2` (Público, licença `CC-BY-SA-4.0`, auditado por `09_release_pipeline.ipynb` com `manifest.json` SHA-256).

**Escopo do registro histórico abaixo:** inspeção estática do estado de `main` após o merge inicial da arquitetura. Esta seção histórica distingue a fotografia inicial no GitHub do estado operacional posteriormente atingido no release v2.

## Resumo executivo

A inspeção encontrou uma base de ingestão e validação relevante, mas ainda não há evidência suficiente para declarar a Bronze integralmente reproduzível/publicada sob a identidade canônica `miolodados/tse-eleicoes-dados`.

| Área | Evidência no repositório | Conclusão |
|---|---|---|
| Catálogo de fontes | `configs/sources.yml` e `docs/source_inventory.md` | Há sete famílias de fontes e metadados de ingestão. |
| Ingestão tabular | Notebook 02 e testes correspondentes | Há implementação e cobertura de testes no GitHub; execução/publicação Kaggle atual não verificada. |
| Logs de urna | Notebook 03, módulos `src/tse_analysis/logs/` e testes | Há parser/materializador e fluxo em lote; configuração padrão limita a ingestão a cinco arquivos `.jez` por partição. |
| Integridade | Notebook 04 e testes | Há auditorias de partições, manifesto, privacidade e reconciliações; fixtures sintéticas não provam cobertura real. |
| Identidade Kaggle | Metadados dos notebooks 01–04 | Os kernels ainda usam IDs pessoais `daniellmattos/tse-analysis-*`; notebook 04 declara como entrada `daniellmattos/tse-analysis-eleicoes-2026`. |
| Publicação | Notebook 03 | `dataset_slug` default aponta para `daniellmattos/tse-analysis-eleicoes-2026` e `execute_push=False`. Não há prova de publicação no dataset canônico. |

## Achados que exigem ação

### 1. A ingestão em lote de logs não está configurada para cobertura integral por padrão

Em `notebooks/03_ingest_urna_logs_batch/03_ingest_urna_logs_batch.ipynb`, a configuração define:

- `TARGET_TURNS = [1, 2]`;
- `TARGET_UFS = list(ALL_UFS)`;
- `MAX_JEZ_FILES_PER_PARTITION = 5`, com comentário explícito de que `None` processaria todos os arquivos `.jez` da UF.

Assim, a configuração padrão percorre UFs e turnos, mas limita a cinco arquivos por partição. Isso é adequado a uma execução rápida de kernel/PoC, não comprova uma ingestão Bronze completa. Não alterei esse limite automaticamente: remover o teto sem medir tempo, memória, volume e estratégia de retomada no Kaggle pode transformar uma correção aparente em falha operacional. A configuração precisa separar claramente modo smoke test de execução de release.

### 2. Os metadados Kaggle ainda não refletem a identidade canônica

Os quatro arquivos `kernel-metadata.json` identificam kernels sob o namespace pessoal `daniellmattos`. Além disso:

- Notebook 04 declara `daniellmattos/tse-analysis-eleicoes-2026` em `dataset_sources`;
- Notebook 03 usa `daniellmattos/tse-analysis-eleicoes-2026` como `dataset_slug` default;
- Notebook 03 mantém `execute_push=False`.

A decisão arquitetural já estabeleceu `miolodados/tse-eleicoes-dados` como identificador canônico. A migração não pode ser considerada concluída enquanto metadados, fontes de entrada, defaults de publicação e permissões/ownership dos kernels não forem alinhados e testados no próprio Kaggle.

### 3. A validação de cobertura precisa ser interpretada por fonte

O notebook 04 contém um catálogo de 18 tabelas e regras de cobertura. A fixture em `tests/test_phase3_integrity_and_release.py`, entretanto, é sintética e deliberadamente pequena (AC, turno 1). Ela testa a lógica do auditor, não a existência de partições reais para todas as UFs/turnos.

A matriz universal de 27 UFs também não deve ser aplicada indiscriminadamente: há fontes nacionais, fontes por UF, fontes com `BR` e fontes com `ZZ`. A expectativa deve vir do contrato de cada fonte e de sua aplicabilidade, não de uma lista única presumida.

### 4. A documentação mistura código existente com publicação de tabelas

O repositório já contém parser, catálogo de eventos e materializador de `fact_urna_event`, além de testes sintéticos ponta a ponta. Isso demonstra implementação de código e comportamento em fixtures, mas não demonstra que `fact_urna_event` foi executada em lote e publicada no Kaggle. O dicionário e o README devem distinguir pelo menos:

- código implementado/testado no GitHub;
- execução concluída no Kaggle;
- artefato efetivamente publicado em uma versão identificável do dataset.

Não declarei `fact_urna_event` publicada nem promovi o status da Silver.

## Próximas ações propostas

1. **Release/Kaggle (#17):** alinhar kernel IDs, dataset sources e destino de publicação; executar no Kaggle com permissões e ownership corretos; registrar versão publicada e manifesto.
2. **Bronze (#12):** separar modo smoke test de modo release, tornar o limite de arquivos explícito nos parâmetros/manifesto e validar cobertura real por fonte/turno/UF.
3. **Docs (#18):** atualizar o dicionário para separar status de implementação do código, execução Kaggle e publicação.
4. **Silver (#13):** antes de promover `fact_urna_event`, validar em lote a cobertura, os desconhecidos/ambíguos e a rastreabilidade no Kaggle.

## Limites desta auditoria

- Nenhum notebook foi executado nesta alteração.
- Nenhum artefato oficial foi gerado ou publicado fora do Kaggle.
- Não houve acesso autenticado ao estado atual dos datasets/kernels no Kaggle.
- Nenhuma regra semântica, chave natural ou expectativa de cobertura foi alterada.
- Os testes do repositório não foram executados neste ambiente; a revisão aqui foi estática.

Esses limites são intencionais: a regra operacional do projeto exige que a execução oficial e a publicação aconteçam no Kaggle.

## Atualização Empírica (Execução Real 2026 via Notebook `09_release_pipeline.ipynb`)

- A auditoria integral das **18 tabelas Bronze** (`8,256,264,342` linhas em 1.192 arquivos Parquet) confirmou que 15/18 tabelas apresentam 0 duplicatas nas amostras verificadas, enquanto 3 tabelas demográficas espelhadas 1:1 dos CSVs brutos do TSE contêm múltiplas linhas por combinação demográfica na origem (`fact_eleitorado_perfil_secao`, `fact_eleitorado_perfil_munzona` e `fact_comparecimento_abstencao`, por variação de lote/subtotal na fonte oficial TSE).
- O pipeline `09_release_pipeline.ipynb` classifica essas 3 ocorrências como `INFO_UPSTREAM_TSE` (informativo não bloqueante na Bronze) e continua exigindo unicidade estrita (`0` duplicatas) em todas as tabelas derivadas **Silver** e **Gold**.
