# Script de vídeo — Pipeline Control-M/Helix B3 + Datadog

**Público:** Karol, Carlos, Fabricio, Leonardo (B3)
**Duração alvo:** 4 minutos
**Formato:** screen recording + voice-over (PT-BR)
**Objetivo:** mostrar que o fork cobre todas as combinações plausíveis do ambiente Helix da B3 e como eles escolhem a variante quando confirmarem SO/Python.

---

## 0:00 — 0:20 · Abertura

**[TELA]** Slide simples ou IDE aberto no `b3/controlm/`.

> "Oi Karol, oi pessoal. Esse vídeo é um recap do que eu adiantei depois das nossas perguntas no WhatsApp.
>
> Vocês confirmaram que usam Helix — Control-M SaaS. As outras três perguntas — SO do agent, tipo de job, Python — ainda estão em aberto.
>
> Em vez de esperar a resposta para começar, eu deixei o pipeline pronto pras quatro combinações possíveis. Quando vocês confirmarem o ambiente, é só apontar para a variante certa."

---

## 0:20 — 0:50 · O manifest único

**[TELA]** Abrir [b3/controlm/jobs/manifest.yaml](../controlm/jobs/manifest.yaml). Destacar a seção `jobs:` com os quatro jobs do pipeline.

> "Esse arquivo aqui é a fonte única da verdade. Ele descreve o pipeline D+1 que vocês vão ver: `close_market_eod`, `reconcile_d1_positions`, `quality_gate_d1` e `publish_d1_reports`.
>
> Cada job declara as tabelas que lê e escreve — Oracle de entrada, SQL Server de destino, e os arquivos no S3 no final. Esses são os nomes reais de produção: `ASTADRVT_TRADE_MVMT` virando `ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO`, e assim por diante.
>
> Toda a observabilidade no Datadog é gerada a partir desse arquivo."

---

## 0:50 — 1:35 · As quatro variantes

**[TELA]** Terminal — rodar:

```bash
make ctm-generate
```

Mostrar a saída e em seguida `ls b3/controlm/jobs/`.

> "Esse comando lê o manifest e gera quatro variantes do pipeline em JSON — uma para cada `Job:Type` que o Control-M suporta:"

**[TELA]** Abrir os 4 arquivos lado a lado (split editor):

- `market_d1_script.json`
- `market_d1_command.json`
- `market_d1_embedded.json`
- `market_d1_database.json`

> "**Script** — quando o `run_job.py` está em disco no agent. Caso clássico.
>
> **Command** — o comando vai inline, sem precisar deploy de arquivo no host.
>
> **EmbeddedScript** — o script inteiro vai dentro do próprio JSON. Útil se o time de plataforma de vocês não quer arquivos espalhados nos agents.
>
> **Database** — sem Python. Roda SQL direto contra Oracle e SQL Server via Connection Profile do Helix. Esse é o plano B caso vocês não tenham Python instalado nos hosts."

---

## 1:35 — 2:10 · Matriz de compatibilidade

**[TELA]** Abrir [b3/controlm/README.md](../controlm/README.md), scroll até "Compatibility matrix".

> "Aqui na matriz, vocês acham a combinação de vocês na coluna da esquerda e descobrem qual variante usar e quais variáveis sobrescrever no Helix Web UI.
>
> Por exemplo — Linux com Python num venv em `/opt/b3-venv`: usem a variante `script`, e no Helix sobrescrevem `PYTHON_BIN` para apontar para esse venv. Pronto.
>
> Windows com `py launcher`: mesma variante, sobrescrevem `PYTHON_BIN=py -3`, `SCRIPT_DIR=C:\controlm\scripts`, e `PATH_SEP=\`.
>
> Sem Python no agent: variante `database`, configuram o Connection Profile no Helix e tá feito."

---

## 2:10 — 2:40 · Adicionar job novo é trivial

**[TELA]** Voltar para o `manifest.yaml`, mostrar uma adição fictícia ao final:

```yaml
  - name: archive_d1
    sla: "00:05"
    depends_on: publish_d1_reports
    description: "Arquivamento dos relatórios D+1"
    inputs:
      - {ns: minio, name: "derivatives/ADWPM_MOVIMENTO_NEGOCIO_DERIVATIVO"}
    outputs:
      - {ns: minio, name: "archive/derivatives/2026"}
```

> "Pra adicionar um job novo no pipeline, vocês editam o manifest aqui, descrevem entradas e saídas..."

**[TELA]** Terminal:

```bash
make ctm-generate
```

> "...e rodam esse comando. As quatro variantes são regeneradas com o job novo incluído, e a tabela de lineage no `run_job.py` também é atualizada automaticamente. Não precisa tocar em mais nenhum arquivo."

---

## 2:40 — 3:30 · O que aparece no Datadog

**[TELA]** Browser aberto no Datadog. Navegar pelos 3 produtos rapidamente.

**Data Jobs Monitoring (DJM):**

> "No Data Jobs Monitoring, vocês veem o pipeline inteiro com status de cada execução, duração, retries e SLA. Os quatro jobs aparecem encadeados conforme as dependências do manifest."

**Data Lineage:**

> "Em Data Lineage, a cadeia completa: tabelas Oracle do ASTA, jobs do Control-M, tabelas SQL Server do ADWPM, e os arquivos no S3 no final. Se algum dia uma linha duplicada chegar nas tabelas de origem, vocês conseguem rastrear até qual arquivo final foi afetado."

**Database Monitoring (DBM):**

> "No DBM, vocês veem as queries que o pipeline está rodando em Oracle e SQL Server — latência, planos de execução, top queries. É aqui que detectamos gargalos antes que virem incidente."

---

## 3:30 — 4:00 · Fechamento + próximos passos

**[TELA]** Voltar pro `README.md`, scroll até "Open questions for B3".

> "Pra fechar, esses são os pontos que ainda preciso confirmar com vocês:
>
> Linux ou Windows nos agents. Qual `Job:Type` o time de vocês padroniza hoje. Python — onde está instalado e em qual versão. Se eventualmente forem usar a variante database, quais são os Connection Profiles do Oracle e do SQL Server no Helix de vocês.
>
> Com essas quatro respostas, eu já consigo apontar exatamente qual variante usar e quais variáveis vocês precisam sobrescrever. Aí a gente parte pro piloto.
>
> Qualquer dúvida me chama no WhatsApp. Valeu!"

---

## Notas de produção

**Gravação:**
- Loom, OBS ou QuickTime — full screen, 1080p mínimo
- Microfone próximo (lavalier ou USB direcional). Áudio claro vale mais que vídeo bonito.
- Preparar antes da gravação:
  - IDE aberto com os arquivos necessários em abas
  - Terminal no diretório `b3/`
  - Datadog aberto em 3 abas: dashboard pipeline D+1, dashboard lineage, dashboard DBM
  - Login no Datadog feito (sem expirar durante a gravação)

**Pré-corrida sugerida** (uma vez antes de gravar para popular o Datadog):
```bash
make up
make demo-happy BUSINESS_DATE=$(date +%F)
```
Aguardar 2-3 min para os widgets do Datadog refletirem.

**Edição:**
- Não precisa cortar — script foi feito pra ser linear.
- Adicionar callouts/zoom em momentos chave: nome das tabelas no manifest, comando `make ctm-generate`, e widgets do Datadog.
- Thumbnail sugerida: print do dashboard DJM mostrando os 4 jobs verdes encadeados.

**Distribuição:**
- Subir no Datadog SharePoint / Google Drive da Datadog
- Link via WhatsApp para Karol marcando próximos passos
- Backup: enviar também por email para o grupo (Carlos, Fabricio, Leonardo)
