# Clima nas Filiais — Open-Meteo → Fabric Lakehouse → Power BI (Direct Lake)

Projeto pessoal de engenharia e análise de dados no **Microsoft Fabric**: dados de clima da API pública
[Open-Meteo](https://open-meteo.com/) passam por uma **arquitetura medalhão** (Bronze → Silver → Gold)
em um Lakehouse e chegam a um relatório Power BI em **Direct Lake**, voltado ao planejamento de
trabalho de campo em 4 cidades do Espírito Santo.

```
Open-Meteo API ──► Notebook PySpark ──► Lakehouse (Delta) ──► Modelo semântico ──► Relatório PBIP
 (90 dias obs.      Nivel1_OpenMeteo_     bronze / fato /       SM_Clima_Filiais      3 páginas
  + 16 previsão)    Medalhao              gold / dims           (Direct Lake)
```

## Estrutura do repositório

| Caminho | O que é |
|---|---|
| `fabric/Nivel1_OpenMeteo_Medalhao.Notebook/` | Notebook principal (PySpark): ingestão, MERGE incremental, qualidade, Gold e calendário |
| `fabric/Teste1.Notebook/` | Primeira versão (protótipo) — mantida para mostrar a evolução e o bug corrigido |
| `fabric/PL_OpenMeteo_Diario.DataPipeline/` | Pipeline de orquestração: executa o notebook todo dia às 05:00 (horário de Brasília) |
| `fabric/SM_Clima_Filiais.SemanticModel/` | Modelo semântico em TMDL (tabelas, relacionamentos e medidas DAX) |
| `DIRECT LAKE METEOROLOGIA API.pbip` + `.Report/` | Relatório Power BI em formato PBIP/PBIR (conexão ao vivo com o modelo) |
| `scripts/gen_report.py` | Script que gerou o layout do relatório (fundos SVG→PNG + páginas e visuais PBIR) |

Os itens em `fabric/` estão no formato de **integração Git do Fabric** (`.platform` + definição),
exportados pela API REST (`getDefinition`), e podem ser sincronizados de volta com um workspace.

## 1. Ingestão e transformação (notebook `Nivel1_OpenMeteo_Medalhao`)

| Etapa | Tabela | Como |
|---|---|---|
| Bronze | `bronze_openmeteo_clima_raw` | JSON **cru** da API por filial, em *append*, particionado por `data_ingestao`. Cada execução é um snapshot (histórico completo do que a API respondeu). |
| Dimensão | `dim_localidade` | 1 linha por filial (nome, UF, latitude/longitude), reconstruída a partir da configuração. |
| Silver / fato | `fato_clima_diario` | Parse com schema explícito, `arrays_zip` + `explode` dos arrays paralelos, classificação `observado`/`previsao`, **deduplicação** por `row_number` (vale a ingestão mais recente) e **MERGE** Delta idempotente, com *watermark* por `timestamp_ingestao`. |
| Gold | `gold_clima_filial` | Chuva acumulada 7 e 30 dias (janelas móveis) e a regra de negócio **`dia_favoravel_campo`** = chuva < 5 mm **e** vento < 30 km/h. |
| Calendário | `dim_calendario` | Gerada no Spark (Direct Lake não permite colunas calculadas): nomes em PT-BR, fim de semana, ano fiscal nov–out (`FY27`) e ordem do mês no FY. |
| Otimização | — | `OPTIMIZE … VORDER` nas tabelas lidas pelo Direct Lake (menos arquivos pequenos, leitura mais rápida, evita *fallback* para DirectQuery). |

**Qualidade de dados**
- `validar_qualidade()` **interrompe** o pipeline se houver chave nula, grão duplicado (filial × dia) ou
  `temperatura_min > temperatura_max` — melhor falhar alto do que gravar dado errado em silêncio.
- Teste negativo: uma cópia corrompida da fato confirma que a checagem realmente bloqueia.
- Auditoria final com 10 checagens *esperado × obtido* (dimensão, duplicidade, buracos de datas,
  órfãos, coerência do `tipo`, watermark, recálculo manual da chuva de 7 dias etc.).

**O bug corrigido.** O protótipo (`Teste1`) gravava a Silver em *overwrite* a partir de toda a Bronze;
como a Bronze recebe um snapshot por execução, o mesmo filial × dia aparecia várias vezes e qualquer
`SUM` no Power BI dobrava os valores. A versão final resolve com deduplicação + MERGE na chave
`(filial_id, data_referencia)`.

## 2. Modelo semântico `SM_Clima_Filiais` (Direct Lake)

- Esquema estrela: `gold_clima_filial` (fato) → `dim_calendario[data]` e `dim_localidade[filial_id]`.
- `latitude`/`longitude` categorizadas como **Latitude/Longitude** para o mapa.
- Medidas (tabela `Medidas`): `Chuva Total (mm)`, `Chuva Observada (mm)`, `Chuva Prevista (mm)`,
  `Chuva Acum 30d (mm)`, `Chuva Próx 7 Dias (mm)`, `Temp Máx Média`, `Temp Mín Média`,
  `Dias Favoráveis`, `% Dias Favoráveis` e **`Cor Dia Campo`** — devolve a cor
  (`#A9CC9A` favorável / `#F4C7C3` desfavorável) usada em formatação condicional por valor de campo.

## 3. Relatório `DIRECT LAKE METEOROLOGIA API` (PBIP / PBIR)

Tema personalizado em tons de verde e dourado; fundos de página desenhados em SVG (cabeçalho com
curvas de nível, cartões de KPI com ícones) e renderizados em PNG; navegação por *page navigator*.

| Página | Filtro | Conteúdo |
|---|---|---|
| **Visão Geral** | — | 5 KPIs, mapa das filiais (bolha = chuva), chuva diária × temperatura máxima com barras coloridas pela `Cor Dia Campo`, % de dias favoráveis por filial |
| **Planejamento de Campo** | `tipo = previsao` | **Calendário de campo** (matriz filial × dia com fundo pela `Cor Dia Campo`), chuva prevista por dia e faixa de temperatura |
| **Histórico** | `tipo = observado` | Filtros de filial e período, chuva × temperatura no tempo, chuva mensal por filial e tabela-resumo |

Para abrir: Power BI Desktop → abrir o `.pbip`. O relatório usa conexão ao vivo com o modelo
publicado no workspace (`definition.pbir`); para usar em outro ambiente, troque a conexão para o seu
modelo semântico.

## 4. Atualização automática

O pipeline `PL_OpenMeteo_Diario` roda **todo dia às 05:00 (America/Sao_Paulo)** e executa o notebook
`Nivel1_OpenMeteo_Medalhao` (timeout de 2 h, até 2 novas tentativas com 5 min de intervalo).
O agendamento próprio do notebook fica desativado para não haver execução dupla.

Como o modelo é **Direct Lake**, não há refresh de importação. A última célula do notebook chama
`sempy.fabric.refresh_dataset("SM_Clima_Filiais")`, que faz o *reframing*: o modelo passa a ler a
versão nova das tabelas Delta logo após a carga, e o relatório mostra os dados do dia sem depender
da atualização automática do Direct Lake.

Fluxo diário: **05:00** pipeline → notebook (API → Bronze → fato → Gold → calendário → OPTIMIZE) →
reframing do modelo → relatório atualizado.

## Observações

- Os `filial_id` são códigos de exemplo; as coordenadas são aproximadas (centro de cada cidade).

## Tecnologias

Microsoft Fabric (Lakehouse, Notebooks PySpark, Data Pipeline) · Delta Lake (MERGE, VORDER) ·
Power BI (Direct Lake, DAX, PBIP/PBIR, TMDL) · Python · API Open-Meteo
