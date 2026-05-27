# B3 — Discovery essencial (6 perguntas)

> Versão enxuta. Para versão completa ver [discovery_b3_questions.md](discovery_b3_questions.md).

## Contexto rápido pro Lairton/Karoline

> Control-M não tem linguagem de script própria. Cada `Job:Command` chama um
> processo do SO (shell, Python, sqlplus, etc.). Para instrumentar com
> Datadog (APM + lineage), precisamos de um processo de aplicação
> instrumentável — normalmente Python ou Java. Por isso as perguntas abaixo.

---

## 1 · On-prem ou Control-M SaaS (Helix)?

- **On-prem:** instrumentação 100% controlada pela B3. Datadog Agent pode rodar no host do Control-M Agent.
- **SaaS:** o Control-M EM/Server fica na BMC, mas os **Control-M Agents continuam on-prem na B3** (rodando os jobs). Não muda muito o plano — instrumentação fica nos Agents mesmo.

---

## 2 · Sistema operacional dos hosts do Control-M Agent — Linux ou Windows?

- **Linux** (RHEL/Oracle Linux mais comum): caminho mais limpo. `ddtrace-run`, Datadog Agent, DBM, tudo nativo.
- **Windows**: tudo funciona, mas paths/env vars/wrappers em PowerShell ou `.bat`. DBM Oracle no Windows requer Instant Client.

Se for misto, qual SO roda o pipeline alvo da POC?

---

## 3 · Que tipo de job o Control-M dispara hoje?

Opções (qual é a B3?):
- `Job:Command` chamando shell script (`bash /opt/.../run.sh`)
- `Job:Command` chamando Python (`/usr/bin/python3 /opt/.../etl.py`)
- `Job:Script` apontando para script de filesystem
- `Job:Database:EmbeddedQuery` ou `StoredProcedure` — SQL direto, sem processo de aplicação
- `Job:FileTransfer` / `Job:FileWatcher`
- Outro tipo customizado (Application Integrator)

> **Esta é a pergunta que mais influencia o plano.** Se a resposta for "SQL direto" precisamos propor um wrapper Python; se for shell ou Python, ganhamos tempo.

---

## 4 · O que tem DENTRO do job (a estrutura do script)?

Idealmente, peça pra mostrar **um exemplo real** do conteúdo do job:
- É um shell script `run_etl.sh` que chama `sqlplus`?
- É um Python `etl.py` que conecta direto via `oracledb` + `pyodbc`?
- É uma stored procedure `EXEC dbo.sp_load_movimento @date='%%ODATE'`?
- É uma sequência de comandos no `EmbeddedScript`?

> Se conseguir um exemplo concreto do pipeline `close_market_eod` ou equivalente real da B3, fechamos o plano em 15 minutos.

---

## 5 · Já tem Python no host? Versão? Onde?

- Python 3.7+ já instalado?
- Em venv? Conda? Global do SO?
- Em quais hosts? (todos os Agent hosts ou só alguns?)
- Quem instala pacotes Python no host hoje?

> Se sim → `pip install ddtrace` resolve. Se não → precisa de aprovação Ops/SecOps para instalar Python + ddtrace.

---

## 6 · Datadog na B3 — conta já existe? Quais SKUs?

- Conta Datadog em produção?
- Site (`datadoghq.com`, `us3.datadoghq.com`, `datadoghq.eu`)?
- SKUs ativos: **APM**, **DBM**, **Logs**, **Data Jobs Monitoring** (Preview)?
- Quem é o CSM da Datadog na conta?
- Já tem Datadog Agent rodando em ALGUM host da B3?

---

## Como cada resposta destrava o que

| Pergunta | Resposta "ideal" | Resposta que complica |
|---|---|---|
| 1 — On-prem/SaaS | On-prem | SaaS → coordenação extra com BMC pra acesso ao EM |
| 2 — SO | Linux | Windows → playbook diferente, instrumentação um pouco mais verbosa |
| 3 — Job type | `Job:Command` shell ou Python | `Job:Database` puro → precisa wrapper novo |
| 4 — Estrutura | Existe processo Python (mesmo dentro de shell) | Apenas SQL/stored procedure → wrapper novo |
| 5 — Python no host | Já tem 3.8+ | Não tem → aprovação Ops/SecOps |
| 6 — Datadog | Conta + APM + DBM + Logs ativos | Sem conta ou sem SKU → upsell/CSM antes de qualquer demo |

---

## Cenário mais provável (predição, a confirmar)

Pela arquitetura típica de instituições financeiras BR + o que já conheço da B3:

- **(1) On-prem** com Control-M EM em datacenter próprio
- **(2) Linux** (RHEL ou Oracle Linux) nos Agent hosts
- **(3)** Mix de `Job:Command` (shell) + `Job:Database` (PL/SQL stored procedures)
- **(4)** Pipelines core: shell script chamando `sqlplus` + scripts PL/SQL. Talvez algum Python pontual em DQ checks.
- **(5)** Python provavelmente instalado no host (3.6 ou 3.8 — versão "default RHEL")
- **(6)** Conta Datadog existente, talvez com APM ativo mas DBM ainda não (oportunidade da POC)

**Se for esse cenário:** o plano se traduz em "criar um pequeno `b3_runner.py` wrapper" que envelopa os jobs existentes com `ddtrace`, `dogstatsd`, OpenLineage emit. Pipelines PL/SQL puro ganham observability via APM (span por execução do SP) + DBM (visão dentro do DB). É factível, elegante, e zero impacto nas stored procedures legadas.

---

## Próximos passos

1. **Marcar reunião com Lairton** (30min é suficiente pra 6 perguntas).
2. Pedir um **exemplo real** de um job do pipeline (JSON do Control-M ou export XML).
3. Com as respostas, fechar o plano de implementação real e estimar timeline.
