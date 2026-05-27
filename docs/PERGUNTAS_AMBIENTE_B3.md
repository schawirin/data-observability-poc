# Perguntas técnicas — Ambiente B3 (Control-M + Pipelines)

> **Objetivo:** validar a arquitetura proposta em `architecture_b3_real.html` contra a realidade do ambiente B3 antes de implementar.
> **Audiência:** Lairton Borges + Ops Control-M + DBA Oracle/SQL Server da B3.
> **Formato sugerido:** 1 call de 45-60min cobrindo blocos 1–6, follow-up async para 7–9.

---

## Bloco 1 · Inventário Control-M 🔴 CRÍTICO

A versão e edição definem o que é tecnicamente possível (Automation API REST disponível? Job:Command nativo? Application Integrator?).

| # | Pergunta | Por que importa | Resposta esperada / impacto |
|---|---|---|---|
| 1.1 | Qual é a versão exata do Control-M EM, Control-M Server e Control-M Agent em produção? | Automation API REST só está disponível a partir de **9.0.20**. Versões anteriores usam só XML legacy e GUI. | Se ≥ 9.0.20 → JSON Job:Command direto. Se < 9.0.20 → migrar export para XML. |
| 1.2 | Vocês usam Control-M **Self-Hosted** (on-prem) ou **Control-M SaaS** (BMC Helix Control-M)? | SaaS limita o que pode rodar no Agent host. | Self-hosted → instalamos Datadog Agent no host livremente. SaaS → Datadog Agent só nos Agent hosts on-prem. |
| 1.3 | Quais SOs dos Control-M Agent hosts? (RHEL 7/8/9? Windows Server?) | Define qual binário Datadog Agent e qual Python instalar. | RHEL ≥ 7 + Python ≥ 3.8 é o ideal. Windows tem suporte mas é mais trabalhoso. |
| 1.4 | Quantos Control-M Agent hosts existem? Em quais zonas/datacenters? Quais hospedam os jobs do D+1 hoje? | Dimensiona footprint Datadog (1 agent por host). | Ideal saber: nomes dos hosts + zone tag. |
| 1.5 | Existem ambientes de homologação/desenvolvimento Control-M antes de produção? Podemos testar lá primeiro? | Reduz risco. | HML separado é mandatório para o POC ser representativo. |

---

## Bloco 2 · Como os jobs são iniciados HOJE 🔴 CRÍTICO

> Esta é a dúvida principal: **o que está no campo `Command` do job hoje?**

| # | Pergunta | Por que importa | Resposta esperada / impacto |
|---|---|---|---|
| 2.1 | Para os 4 jobs do D+1 (`close_market_eod`, `reconcile_d1_positions`, `quality_gate_d1`, `publish_d1_reports` — ou equivalentes reais), qual é o **Job Type** no Control-M? `Job:Command`? `Job:Script`? `Job:Database:EmbeddedQuery`? `Job:Database:SQLScript`? | Define onde colocar o `ddtrace-run`. Job:Database executa SQL puro no servidor — não passa por Python no Agent host. | Se `Job:Database` → APM precisa ser via DBM (já temos). Se `Job:Command`/`Job:Script` → prefixamos `ddtrace-run`. |
| 2.2 | **Mostre exatamente o que está no campo `Command` (ou `FileName` + `Arguments`) de cada job hoje.** | Define se já é Python, shell wrapper, executável .NET, etc. Ex: `python /opt/etl/run.py %%ODATE` ou `/opt/scripts/job_runner.sh close_market_eod` ou `sqlplus user/pass @/opt/sql/close.sql` | Determina se basta prefixar `ddtrace-run` ou se precisa refatorar o wrapper. |
| 2.3 | Se há um **shell script wrapper** (ex: `runner.sh`), o que ele faz antes/depois do payload? Export de env vars? Source de venv? Log redirect? | Onde injetar `export DD_SERVICE/DD_ENV/DD_TAGS`. | Se já existe wrapper → adicionamos exports lá (1 linha). Se job chama Python direto → usamos PreCommand do Control-M. |
| 2.4 | Se já é Python: qual versão (3.x)? Como o virtualenv é gerenciado no Agent host? Pip mirror interno? | Define se conseguimos instalar `ddtrace` e `openlineage-python` via pip. | Ideal: Python ≥ 3.10 + pip interno. Se tem ambiente air-gapped, precisamos pacote whl distribuído. |
| 2.5 | Se NÃO é Python (ex: shell + sqlplus, .NET, Java): considerariam migrar para Python para esses 4 jobs? Ou prefere outro caminho? | Define se "Python + ddtrace" é viável ou se temos que usar APM Java/dotnet/sh-only. | Resposta determina toda a arquitetura. |

---

## Bloco 3 · Variáveis e parâmetros Control-M 🟡 IMPORTANTE

| # | Pergunta | Por que importa |
|---|---|---|
| 3.1 | Quais variáveis Control-M estão disponíveis nos jobs D+1? `%%ODATE`, `%%ORDERID`, `%%JOBNAME`, `%%FOLDER`, custom? | Cada uma vira tag Datadog. Quanto mais granular, melhor o filtro. |
| 3.2 | Como `business_date` é passado ao Python hoje? `%%ODATE` direto, ou parameter custom? Há manipulação de fuso/calendar? | Replicar fielmente na instrumentação. |
| 3.3 | Existem **smart folders** ou apenas folders simples? Algum cycle (D+1 acionado por evento upstream)? | Define se o lineage precisa cruzar folders. |

---

## Bloco 4 · Sysout / logging hoje 🟡 IMPORTANTE

| # | Pergunta | Por que importa |
|---|---|---|
| 4.1 | Onde o Control-M Agent grava o sysout dos jobs? Path completo (ex: `/var/opt/ctm/sysout/`)? | Datadog Agent precisa do path para `logs:` tail. |
| 4.2 | Política de retenção/rotação do sysout? Após N dias é arquivado/deletado? | Define janela máxima de logs no Datadog. |
| 4.3 | Já existe coleta de sysout para algum sistema (Splunk, Elastic, ArcSight)? Vão substituir ou conviver? | Convivência exige tags claras para evitar dupla cobrança. |
| 4.4 | Hoje, quando um job falha, **quem fica sabendo e como**? Email? Slack? PagerDuty? Pessoa olha no Control-M? | Define migração de monitores existentes. |
| 4.5 | O Python (ou wrapper) hoje grava log próprio fora do sysout (ex: `/var/log/etl/job.log`)? | Se sim, precisamos tail dele também. |

---

## Bloco 5 · Datadog account e SKUs 🔴 CRÍTICO

| # | Pergunta | Por que importa |
|---|---|---|
| 5.1 | A B3 já tem uma org Datadog em produção? Qual o site (us1/us3/us5/eu)? Qual o nome da org? | Determina endpoint de envio (`datadoghq.com`, `us3.datadoghq.com`, `datadoghq.eu`). |
| 5.2 | Quais SKUs estão contratados? APM? Database Monitoring? Log Management? **Data Jobs Monitoring (DJM)**? | Sem APM → instrumentação via spans não rende. Sem DJM → lineage view fica vazia (a tela do print que você viu). |
| 5.3 | Vocês têm CSM (Customer Success Manager) Datadog atribuído? Ele pode habilitar features em **Preview** (como DJM se ainda for Preview)? | DJM tem sido Preview em algumas orgs — precisa enable explícito. |
| 5.4 | Vocês já têm Datadog Agent instalado em servidores B3 (não Control-M)? Quem aprova? Quanto tempo demora? | Reduz fricção: se já instala em outros hosts, escalar para Control-M host é incremental. |
| 5.5 | Qual a política de tagging Datadog na B3? (ex: `team:`, `env:`, `service:`, `cost_center:`) | Para honrar o padrão deles e aparecer nos cost reports corretamente. |

---

## Bloco 6 · Datadog Agent no host do Control-M Agent 🔴 CRÍTICO

> Esta é a maior **decisão de aprovação Ops** da implementação.

| # | Pergunta | Por que importa |
|---|---|---|
| 6.1 | Ops B3 aceita instalar o **Datadog Agent** no host onde roda o Control-M Agent? Existe processo formal? | Define se vamos com Padrão A (sem agent local) ou Padrão A+B (recomendado). |
| 6.2 | Se sim — qual processo (ticket, change request)? Qual SLA típico? | Cronograma do POC. |
| 6.3 | Se não — o host tem acesso outbound HTTPS para `*.datadoghq.com` (ou site equivalente)? Via proxy corporativo? | Padrão A puro precisa de outbound HTTPS direto ou via proxy. |
| 6.4 | Existe whitelist de domínios? Conseguem adicionar Datadog (`*.datadoghq.com`, `*.dd-cdn.com`)? | Sem whitelist, sem ingestion. |
| 6.5 | O host do Control-M Agent compartilha rede com os bancos (Oracle/SQL Server) ou está em DMZ separada? | DBM precisa Datadog Agent com rota TCP até o DB. Se o Control-M Agent host tem essa rota → Agent local cobre tudo. |

---

## Bloco 7 · Bancos de dados (Oracle, SQL Server) 🟡 IMPORTANTE

| # | Pergunta | Por que importa |
|---|---|---|
| 7.1 | Versões exatas em produção (Oracle Enterprise 19c/21c? SQL Server 2019/2022?) | DBM tem requisitos mínimos (Oracle 12.2+, SQL Server 2014+). Confirma compatibilidade. |
| 7.2 | DBA aceita criar usuário `datadog` com grants DBM? (V$ views, DBMS_WORKLOAD_REPOSITORY no Oracle; sys.dm_*, performance counters no SQL Server) | Sem grants, DBM não funciona. Boa notícia: são read-only. |
| 7.3 | Conexão TLS/SSL obrigatória? Wallet Oracle / certificate SQL Server? | Define como configurar o Datadog Agent (config tem campos para TLS). |
| 7.4 | Algum banco fica em rede isolada (sem rota direta do host do Agent)? | Pode precisar de Agent dedicado ou Datadog Private Location. |
| 7.5 | Já têm DBM ou monitoramento de queries hoje (OEM, SCOM, custom)? Vão substituir? | Convivência vs migração. |

---

## Bloco 8 · Pipeline real (validação do POC) 🟢 DESEJÁVEL

> Para confirmar que os 4 jobs do POC refletem a realidade — pode ser que existam 20 jobs no pipeline real e nosso recorte é simplificado demais.

| # | Pergunta | Por que importa |
|---|---|---|
| 8.1 | Os 4 jobs do POC (close, reconcile, quality, publish) correspondem ao fluxo D+1 real ou foi uma simplificação para a demo? | Se simplificação, queremos saber os jobs reais para o lineage. |
| 8.2 | Quantos jobs existem no fluxo D+1 real? Qual o critical path? | Dimensiona dashboard e monitores. |
| 8.3 | SLA real do D+1? "Tudo pronto até XX:00 BRT"? | Cria SLO no Datadog correspondente. |
| 8.4 | Quais tabelas Oracle source e SQL Server target existem (além de ASTADRVT_*/ADWPM_*)? | Lineage cobre exatamente o que importa. |
| 8.5 | Há jobs de outros pipelines (não D+1) rodando no mesmo Control-M Agent? Eles devem entrar no monitoramento? | Define escopo do POC: só D+1, ou tudo no host? |

---

## Bloco 9 · Governança e segurança 🟢 DESEJÁVEL

| # | Pergunta | Por que importa |
|---|---|---|
| 9.1 | Quem aprova mudança em `Job:Command` de jobs em produção? Change Advisory Board? | Cronograma. |
| 9.2 | Há requirement de não logar PII em qualquer ferramenta externa? | Define scrubbing rules no Datadog Agent (logs `processing_rules.mask_sequences`). |
| 9.3 | Datadog Agent / sysout pode conter dados sensíveis (CPF, contas)? Precisamos de mascaramento? | Configurar log scrubbing antes de qualquer captura prod. |
| 9.4 | Há ferramenta de catálogo/lineage hoje (Purview, Collibra, dbt-docs)? Vão integrar? | Define se Datadog Lineage substitui ou complementa. |
| 9.5 | OpenLineage já foi avaliado? Time de dados já produz eventos por outras vias? | Pode haver decisão estratégica já tomada. |

---

## Bloco 10 · Limites práticos de mudança 🟡 IMPORTANTE

> Para calibrar o escopo realista do POC.

| # | Pergunta | Por que importa |
|---|---|---|
| 10.1 | Para o POC, podem editar `Job:Command` em **homologação** sem CAB? E em produção? | Define se POC fica restrito a HML. |
| 10.2 | Podem instalar pip packages (ddtrace, openlineage-python) no Agent host de HML? | Sem isso, instrumentação Python não funciona. |
| 10.3 | Podem criar usuário Datadog em Oracle/SQL Server de HML? | DBM em HML. |
| 10.4 | Há limite de footprint de RAM/CPU no Agent host para um Datadog Agent? | Datadog Agent típico: 200-400MB RAM, < 1% CPU. Saber o headroom. |
| 10.5 | Quanto tempo dura tipicamente um ciclo: solicitar acesso → aprovação → instalação? | Cronograma do POC. |

---

## Top 5 perguntas se a call for de 15min

Se você tiver tempo curto, foque nestas (todas críticas):

1. **(2.2)** Cole exatamente o `Command` de um dos jobs D+1 hoje.
2. **(1.1)** Versão exata do Control-M EM/Server/Agent.
3. **(5.1 + 5.2)** Org Datadog existe? Quais SKUs? Site?
4. **(6.1)** Ops aceita Datadog Agent no host do Control-M Agent?
5. **(7.2)** DBA aceita criar user `datadog` com grants em Oracle/SQL Server prod?

Com isso vocês conseguem decidir: **Padrão A vs A+B**, **HML vs Prod**, **Lineage habilitável vs não**, **timeline realista**.

---

## Como apresentar pra eles

Sugestão de abertura:
> "Mostramos o POC funcionando localmente com Control-M Workbench. Antes de planejar o port para o ambiente de vocês, queremos confirmar 5 coisas críticas que determinam a arquitetura ótima. Depois mando o resto async para o time Ops."

Se eles perguntarem "por que precisa de tudo isso?", a resposta direta é:
> "Quero evitar prometer algo que não rode no Control-M de vocês. A documentação BMC suporta 100% do que mostramos, mas detalhes como versão, política de instalação de agent e formato do Command hoje mudam o caminho de implementação."

---

## Saídas esperadas após responder essas perguntas

Com as respostas, conseguimos entregar:

1. **Documento de implementação** (`PRODUCTION_DEPLOYMENT.md`) com JSON `Job:Command` exato adaptado ao formato deles.
2. **Lista de pré-requisitos para Ops** (instalações, grants, whitelist) — pronta para virar tickets.
3. **Cronograma POC realista** (HML → Prod) baseado em SLAs deles.
4. **Decisão Padrão A vs A+B** com base em resposta a 6.1.
5. **Plano de habilitação DJM** (com CSM Datadog) — paralelo ao código.
