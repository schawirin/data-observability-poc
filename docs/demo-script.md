# Data Observability POC - Demo Script

## Pre-requisitos

1. Docker Desktop rodando
2. `.env` configurado com `DD_API_KEY` valido
3. Stack subiu com `make up && make seed`
4. Aguardar ~30s para MySQL healthcheck e Agent conectar

## Cenario A: Happy Path

**Objetivo:** Mostrar o pipeline completo funcionando, com visibilidade end-to-end.

### Execucao

```bash
make demo-happy BUSINESS_DATE=2026-03-16
```

### O que mostrar no Datadog

1. **Data Jobs Monitoring**
   - Abrir DJM → Pipelines
   - Filtrar por `pipeline:market-d1`
   - Mostrar os 4 jobs em sequencia: `close_market_eod` → `reconcile_d1_positions` → `quality_gate_d1` → `publish_d1_reports`
   - Clicar em cada job: duracao, status, retries

2. **Lineage**
   - No DJM, clicar em um job e abrir a aba de lineage
   - Mostrar o fluxo: `raw_trades` → job → `staging_settlement_recon` → job → `ops_quality_results` → job → `d1_settlement_report.parquet`
   - Destacar as 3 camadas: raw → staging → curated/export

3. **DBM**
   - Abrir Database Monitoring → MySQL → demo-mysql
   - Mostrar as queries executadas durante o pipeline
   - Mostrar latencia normal

4. **MinIO**
   - Abrir http://localhost:9001 (minioadmin/minioadmin)
   - Navegar ate `mock-exchange/exports/dt=2026-03-16/`
   - Mostrar os arquivos `d1_settlement_report.parquet` e `participant_exposure_report.csv`

---

## Cenario B: Falha de Qualidade (Duplicidade)

**Objetivo:** Mostrar deteccao de problema de qualidade e rastreabilidade ate o dataset impactado.

### Execucao

```bash
make demo-fail BUSINESS_DATE=2026-03-17
```

### O que mostrar no Datadog

1. **DJM**
   - `close_market_eod`: SUCCESS
   - `reconcile_d1_positions`: SUCCESS
   - `quality_gate_d1`: **FAILED** (destaque)
   - `publish_d1_reports`: **FAILED** (bloqueado pelo gate)

2. **Monitors**
   - Monitor "Quality Gate Critical" disparou
   - Mostrar o alerta com detalhes: check_name=uniqueness, target_table=raw_trades

3. **Lineage - Impacto**
   - Partir do dataset `raw_trades` (onde o problema nasceu)
   - Mostrar que `staging_settlement_recon` foi gerado com dados ruins
   - Mostrar que `quality_gate_d1` detectou o problema
   - Mostrar que `d1_settlement_report.parquet` NAO foi gerado

4. **Pergunta chave para an exchange:**
   > "Se isso acontecesse em producao, quanto tempo levaria para descobrir que o arquivo nao foi gerado e rastrear ate a causa raiz?"

---

## Cenario C: Gargalo de Banco

**Objetivo:** Mostrar correlacao entre query lenta e atraso no pipeline.

### Execucao

```bash
make demo-slow BUSINESS_DATE=2026-03-18
```

### O que mostrar no Datadog

1. **DBM**
   - Abrir Database Monitoring → Query Samples
   - Mostrar a query de reconciliacao com full table scan
   - Mostrar explain plan sem indice em `raw_settlement_instructions.trade_id`
   - Comparar latencia com execucoes anteriores

2. **DJM**
   - `reconcile_d1_positions` com duracao significativamente maior
   - Possivel SLA miss

3. **Correlacao**
   - Mostrar no dashboard: pico de latencia no MySQL coincide com execucao do job
   - Mostrar que o atraso propagou para jobs downstream
   - **Historia:** job atrasou → query lenta → dataset final atrasado → SLA perdido

4. **Pergunta chave:**
   > "Hoje, como voces correlacionam um problema de performance no banco com um atraso no pipeline orquestrado pelo Control-M?"

---

## Pontos de Discussao

### Para an exchange

- DJM resolve a visibilidade dos jobs do Control-M via OpenLineage
- Lineage mostra impacto upstream/downstream sem precisar de ferramenta separada
- DBM correlaciona performance de banco com saude do pipeline
- Quality gates integrados ao pipeline com alertas automaticos

### Proximos passos sugeridos

1. Conectar Control-M real via OpenLineage
2. Apontar DBM para instancia de staging/dev
3. Expandir quality checks para requisitos da Karol (56 itens)
4. Configurar dashboards customizados por area (risco, liquidacao, compliance)
