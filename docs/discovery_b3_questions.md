# B3 — Discovery técnico (Control-M + pipelines)

**Objetivo:** apurar como o Control-M e os pipelines rodam na B3 hoje, pra fechar o plano de instrumentação Datadog (APM/DBM/Logs/OpenLineage).
**Tempo previsto:** 30-45min com Lairton (tech lead) + Karoline (operações).
**Próxima reunião sugerida:** antes da próxima sessão de POC.

> Cada pergunta tem: **(pergunta)** + *por que importa* + **impacto no plano se ≠ esperado**.

---

## A · Control-M — Versão e topologia

**A1.** Qual é a versão exata do **Control-M EM** e **Server** em produção?
- *Por que:* features mudam por versão. Automation API (REST/JSON) só ≥ 9.0.18. Python client `ctm-python-client` ≥ 9.0.20.
- *Impacto:* < 9.0.18 → demos via Workbench podem não bater 1:1 com o que vão usar; mudanças ficam só via XML legado / UI.

**A2.** O Control-M roda **on-prem** ou **Control-M SaaS (Helix)**?
- *Por que:* SaaS muda a forma de instalar Agent Datadog (precisa ser no host do Application/Agent customer-side, não no SaaS).
- *Impacto:* SaaS → instrumentação só funciona no lado deles (Agent host). Logs/sysout podem ficar limitados ao que o Helix expõe via API.

**A3.** Quantos **Control-M Agent hosts** estão envolvidos no pipeline B3 que vamos demonstrar?
- *Por que:* Datadog Agent precisa ser instalado em cada host onde rodam jobs do escopo. 1 host = 1 install; N hosts = aprovação de fleet.
- *Impacto:* Muitos hosts → considerar deploy via Ansible/Puppet; conversar com Ops sobre golden image.

**A4.** Tem **Control-M Workbench** ou ambiente de staging onde podemos rodar a POC antes de mexer em produção?
- *Por que:* validar instrumentação sem risco. Acelera entrega.
- *Impacto:* sem staging → POC tem que ser num Workbench/sandbox que a gente sobe (já temos no docker-compose).

---

## B · Como o pipeline B3 roda HOJE (essencial pra o plano)

**B1.** Os jobs que processam o pipeline **Oracle → SQL Server** são definidos como qual **Job Type** no Control-M?
- Opções típicas: `Job:Command` (shell), `Job:Script` (script de filesystem), `Job:EmbeddedScript` (inline), `Job:Database:EmbeddedQuery` (SQL direto), `Job:Database:StoredProcedure`, `Job:Hadoop:Spark:Python`, `Job:ApplicationIntegrator:<custom>`.
- *Por que:* define exatamente onde podemos inserir o `ddtrace-run`. Se for `Job:Database:EmbeddedQuery` puro (SQL no JSON), **não há processo Python pra instrumentar** — vamos precisar mudar o tipo do job ou usar wrapper.
- *Impacto crítico:* esta resposta dita o **caminho A vs B** da arquitetura.

**B2.** Hoje o job dispara um **script shell** que chama Python, **executa Python direto**, ou **executa SQL/PL-SQL/T-SQL direto** sem código aplicação?
- Exemplos:
  - `bash /opt/b3/run_etl.sh %%ODATE` → script shell que chama Python dentro
  - `/usr/bin/python3 /opt/b3/etl.py --date %%ODATE` → Python direto
  - `EXEC dbo.sp_load_movimento @date='%%ODATE'` → stored procedure (sem Python)
  - `sqlplus user/pass@db @/opt/b3/loader.sql` → SQL script sem Python
- *Por que:* APM/OpenLineage precisam de processo Python (ou Java) instrumentável. SQL puro → instrumentação tem que vir do Datadog Agent observando o DB (DBM faz, mas perde lineage por job).
- *Impacto:* SQL/SP puro → precisamos propor **wrapper Python** que invoca o SP existente. Aumenta escopo.

**B3.** Vocês têm um **wrapper genérico** (script comum) que envolve todos os jobs do pipeline (logging, exit handling, retry), ou cada job tem seu próprio invocador?
- *Por que:* wrapper genérico = um único ponto onde adicionamos instrumentação. Sem wrapper = mudar N jobs.
- *Impacto:* Sem wrapper → bom momento pra propor um pequeno wrapper Python (`b3_runner.py`) que vira o template padrão.

**B4.** O Python (ou outra linguagem) usado nos jobs está em **virtualenv**, conda env, ou Python global do host?
- *Por que:* `ddtrace` precisa estar instalado no mesmo Python que executa o job. Virtualenv requer ativação no `PreCommand`.
- *Impacto:* venv → `PreCommand: source /opt/b3/venv/bin/activate`. Conda → `conda activate`. Global → `pip install ddtrace` no host.

**B5.** Qual a versão do **Python** rodando os jobs hoje?
- *Por que:* `ddtrace` Python suporta 3.7-3.13 (mais recente). Python 2.x não é suportado.
- *Impacto:* Python 2 → bloqueio total, precisaria migrar.

**B6.** Como os jobs **leem parâmetros** (data de negócio, etc.)?
- Via Control-M Variables (`%%ODATE`, `%%PARM1`)?
- Via env vars exportadas em `PreCommand`?
- Hardcoded?
- Via SET/CTL files (`%%CTM_VAR`)?
- *Por que:* Control-M Variables são a forma "blessed". Se eles já usam, traz `%%ORDERID`, `%%JOBNAME` pra Datadog é trivial.
- *Impacto:* se hardcoded → propor migração pra Variables, ganha tags automáticas.

**B7.** Existe **logging estruturado** hoje (JSON logs) ou stdout livre?
- *Por que:* JSON → Datadog parseia em facets automaticamente. Stdout livre → precisa grok pipeline.
- *Impacto:* sem logs estruturados → propor `python-json-logger` no wrapper.

---

## C · Host / runtime / conectividade

**C1.** Os Control-M Agent hosts são **Linux** (qual distro/versão?), **Windows**, ou mistos?
- *Por que:* Datadog Agent instala em ambos, mas DBM Oracle requer Instant Client (mais fácil em Linux). Comandos de instrumentação diferem.
- *Impacto:* Windows → revisar paths e env vars no plano.

**C2.** O host do Control-M Agent tem **outbound HTTPS** liberado pra `*.datadoghq.com` (ou `*.datadoghq.eu` se for site EU)?
- *Por que:* Datadog Agent sai pelo 443. Sem isso, dados não saem.
- *Impacto:* sem outbound → precisa proxy HTTP corporativo configurado no Datadog Agent.

**C3.** Há **proxy corporativo** obrigatório saindo da rede da B3?
- *Por que:* Datadog Agent suporta proxy (`proxy.http`, `proxy.https` em `datadog.yaml`), mas precisa configurar.
- *Impacto:* sim → coletar credenciais/URL do proxy antes do deploy.

**C4.** Como hoje **logs aplicação** desses jobs são coletados (Splunk? ELK? só sysout do Control-M?)?
- *Por que:* se já tem Splunk/ELK, faz sentido enviar mesmos logs pro Datadog em paralelo (sem dual-write se Datadog Agent fizer tail nativo).
- *Impacto:* Splunk fica → Datadog tail do mesmo sysout. Não fica → propor Datadog como ferramenta única.

**C5.** Política de **segurança/SSL** no host: TLS 1.2/1.3 obrigatório? Cert customizado (self-signed) na cadeia?
- *Por que:* Datadog Agent usa TLS 1.2+ por padrão. Cert chain customizada exige `tls_root_certs` config.
- *Impacto:* cert customizado → adicionar no install playbook.

---

## D · Bancos de dados

**D1.** Versão **Oracle** (Enterprise/Standard/XE? versão major)?
- *Por que:* DBM oficial é Oracle Enterprise 12c+. Standard tem limitações. XE tem o bug `OP_FLAGS NULL` que vimos.
- *Impacto:* Enterprise → tudo funciona. Standard → query samples pode requer workaround. XE → não é prod, não esperado.

**D2.** Versão **SQL Server** e modo de auth (SQL Auth vs Windows Auth)?
- *Por que:* DBM SQL Server suporta SQL Auth completo; Windows Auth requer driver ODBC com Kerberos.
- *Impacto:* Windows Auth → mais complexo, talvez Service Account separada.

**D3.** Quem é o **DBA owner** e tem autonomia pra criar user `datadog` com grants DBM?
- *Por que:* DBM precisa user dedicado com `SELECT_CATALOG_ROLE` (Oracle), `VIEW SERVER STATE` (SQL Server). Quem aprova?
- *Impacto:* DBA disponível → grants em 1 dia. Senão → bloqueio.

**D4.** As tabelas reais (`ASTADRVT_TRADE_MVMT`, `ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO`, etc.) estão nos schemas/databases que a gente vê no JSON da memória, ou são nomes ilustrativos?
- *Por que:* o emitter OpenLineage e os custom queries no Oracle config referenciam esses nomes — se forem diferentes, o lineage view e métricas de row count vão ficar erradas.
- *Impacto:* nomes diferentes → editar JOB_IO + custom_queries (low effort, mas tem que fazer).

**D5.** **Tamanho dos jobs:** quantas linhas movem por execução? (1k? 1M? 100M?)
- *Por que:* dimensiona impacto da instrumentação. Span overhead irrelevante em jobs longos, mas em batch trivial pode dobrar duration.
- *Impacto:* jobs muito leves → considerar sampling APM.

---

## E · Datadog (estado atual na B3)

**E1.** A B3 já tem **conta Datadog em produção**? Qual site (`datadoghq.com` us1 / `us3` / `eu1`)?
- *Por que:* endpoints mudam por site. Org existente acelera tudo.
- *Impacto:* sem org → começar com trial; com org → CSM da Datadog é nosso contato.

**E2.** Quais **SKUs** estão habilitados hoje? (Infrastructure, APM, DBM, Logs, RUM, DJM, Lineage)
- *Por que:* DBM e DJM Lineage são pagos separados. Sem SKU → demo limitada.
- *Impacto:* DBM faltando → bloqueio crítico. DJM Lineage faltando → fallback Service Map.

**E3.** Existe **Datadog Agent** rodando em ALGUM host da B3 hoje? Em produção?
- *Por que:* se já tem fleet de agents, instalar mais um no host Control-M é trivial (Ansible/Puppet playbook existente).
- *Impacto:* fleet existente → ponto positivo. Greenfield → mais aprovação Ops.

**E4.** Quem é o **CSM** (Customer Success Manager) da Datadog na B3?
- *Por que:* DJM Lineage está em Preview — precisa de enable manual pelo CSM. APM/DBM/Logs SKU também passam por ele.
- *Impacto:* sem CSM mapeado → Ana Olgas (AE) é o caminho.

**E5.** A B3 tem **dashboards Datadog próprios** hoje? Quem é o owner?
- *Por que:* não conflitar com naming/tagging existente. Reusar tags se houver convenção (e.g., `team:b3-data`, `domain:capital-markets`).
- *Impacto:* convenção existente → ajustar nosso dashboard pra encaixar.

---

## F · Governance, segurança, aprovações

**F1.** Quem aprova mudança no **campo Command** de um job Control-M em produção?
- DBA owner? SRE? Change Advisory Board?
- *Por que:* nosso plano altera só esse campo. Saber o approver = saber o caminho.
- *Impacto:* CAB → janela de mudança formal. SRE direto → execução rápida.

**F2.** Existe processo de **change management** (ITIL/CAB/JIRA) pra mudança em produção? Tempo médio de aprovação?
- *Por que:* dimensiona timeline da POC → produção.
- *Impacto:* CAB de 2 semanas → priorizar staging.

**F3.** Quem aprova **instalação de software novo** no host (Datadog Agent)? SecOps? Infra?
- *Por que:* Padrão A+B depende disso.
- *Impacto:* aprovação difícil → forçar Padrão A puro.

**F4.** **Segurança** tem requisitos sobre coleta de logs/queries? PII em queries? Redaction?
- *Por que:* DBM coleta query samples — podem conter valores literais. Datadog faz obfuscação automática mas pode não cobrir tudo (custom UDF, etc).
- *Impacto:* PII em logs → habilitar `log_processing_rules` com `mask_sequences`.

**F5.** **Audit trail:** precisamos logar quem disparou um job no Datadog? (compliance financeiro)
- *Por que:* B3 é bolsa, auditoria forte. RUN tag com user é trivial; assinatura mais forte requer SIEM integration.
- *Impacto:* compliance forte → considerar SIEM (Datadog Cloud SIEM se SKU OK, ou export pra Splunk).

---

## G · Volume, SLA, janelas

**G1.** Quantos jobs por dia rodam no pipeline B3 alvo da POC? Pico?
- *Por que:* dimensiona DJM (cada run conta) e custos Datadog (APM/DBM/Logs por GB/host).
- *Impacto:* 100/dia → barato. 100k/dia → conversa sobre filtragem/sampling.

**G2.** Qual é o **SLA do pipeline D+1**? Hora fim esperada?
- *Por que:* monitor de SLO precisa desse threshold. "Pipeline OK antes de 06:00 BRT" vira métrica.
- *Impacto:* SLA flexível → menos alertas; SLA crítico → mais monitors P1.

**G3.** Quais são as **janelas de manutenção/blecaute** onde a instrumentação pode ser deployada?
- *Por que:* Padrão A+B (instalar agent) precisa janela. Padrão A (só Command edit) pode ir num CR normal.
- *Impacto:* janelas raras → Padrão A primeiro pra ganhar tempo, A+B depois.

**G4.** Têm cenários históricos de **incidente em pipeline B3** que poderíamos reproduzir na POC? (ex: travada em deadlock, slow query, falha de retry)
- *Por que:* nossos faults já existentes (db_blocking, db_deadlock, oracle_timeout, gate_fail_hard) ficam mais convincentes se baseados em casos reais.
- *Impacto:* cliente vê valor imediato → POC virar contrato.

---

## H · Sobre o pessoal e o produto

**H1.** Lairton vai estar disponível pra reuniões técnicas semanais durante o POC?
- *Por que:* tech lead engajado = POC sucede. Sem engajamento = atrito.

**H2.** Quem do lado da **plataforma de dados** (não SRE) é decision maker pra adoção?
- *Por que:* Lairton entende SRE mas a compra costuma envolver gerente de dados/CIO.

**H3.** Tem **competidores** na mesa pra essa solução? (Splunk Observability, Dynatrace, New Relic, IBM Databand)?
- *Por que:* comparativo influencia narrativa da demo. Databand é o concorrente direto pra lineage (e tem parceria oficial com BMC).

---

## Atalho — perguntas "must-have" se só rolar 15min

Se a janela for curta, peça respostas a estas **5 perguntas críticas**:

1. **B1 + B2**: Que job type? Shell wrapper, Python direto ou SQL puro?
2. **B4 + B5**: Python disponível no host? Qual versão? Venv ou global?
3. **C1**: Linux ou Windows nos Agent hosts?
4. **E1 + E2**: Datadog org existente? Quais SKUs?
5. **D3**: DBA pode criar user `datadog` com grants DBM?

Com essas 5 respostas, conseguimos finalizar o plano de implementação real.

---

## O que entregamos depois dessas respostas

Com base nas respostas, produzimos:
1. **`controlm/jobs/<job_name>.json`** — JSON do Job:Command pronto pra import (Automation API)
2. **`controlm/PRODUCTION_DEPLOYMENT.md`** — passo-a-passo de migração POC → B3 real
3. **Wrapper Python (`b3_runner.py`)** com instrumentação ddtrace + DogStatsD + OpenLineage incorporados (se B2 não tiver Python hoje)
4. **Lista de grants DBM** por banco, prontos pra DBA aplicar
5. **Datadog Agent install playbook** (Ansible/manual) para Padrão A+B
6. **Cronograma de implementação** com dependências e responsáveis
