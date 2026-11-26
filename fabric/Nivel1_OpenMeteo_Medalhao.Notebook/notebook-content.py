# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "86cfb782-b6af-41d9-af59-223a7110e452",
# META       "default_lakehouse_name": "LH_Teste",
# META       "default_lakehouse_workspace_id": "18c7a711-c0f6-4987-a814-a4b05309954d",
# META       "known_lakehouses": [
# META         {
# META           "id": "86cfb782-b6af-41d9-af59-223a7110e452"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Nível 1 — Pipeline Open-Meteo (Bronze → Silver → Gold)
# 
# **Fluxo deste notebook**
# 
# | Célula | Tarefa | Tabela |
# |---|---|---|
# | 1 | Configuração | — |
# | 2 | Tarefa 0 — provar o bug de duplicidade | `silver_openmeteo_clima` (antiga) |
# | 3–4 | Tarefa 1 — Bronze com 30 dias passados + 16 de previsão | `bronze_openmeteo_clima_raw` |
# | 5 | Tarefa 2 — dimensão de filiais | `dim_localidade` |
# | 6–9 | Tarefas 3 e 4 — fato incremental com MERGE + qualidade | `fato_clima_diario` |
# | 10 | Validação dos critérios de aceite | — |
# | 11 | Tarefa 5 — Gold | `gold_clima_filial` |
# | 12 | Teste: forçar a checagem de qualidade a falhar | — |
# 
# > Anexe o Lakehouse `LH_Teste` ao notebook antes de rodar.

# CELL ********************

# =============================================================================
# CÉLULA 1 — CONFIGURAÇÃO
# Tudo que pode mudar (filiais, nomes de tabela, janelas de dias) fica aqui em
# cima. Assim o resto do código não tem "número mágico" espalhado.
# =============================================================================
import json
import requests

from pyspark.sql import Row, Window
from pyspark.sql import functions as F          # padrão de mercado: F.col, F.sum...
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, DoubleType
)
from delta.tables import DeltaTable              # API Python do Delta (usada no MERGE)

# TODO: troque os filial_id pelos códigos reais do ERP (ex.: 010101).
FILIAIS = [
    {"filial_id": "0101X", "nome": "Linhares",     "uf": "ES", "lat": -19.4021, "lon": -40.0729},
    {"filial_id": "020X",  "nome": "Colatina",     "uf": "ES", "lat": -19.5386, "lon": -40.6306},
    {"filial_id": "0102X", "nome": "Nova Venecia", "uf": "ES", "lat": -18.7139, "lon": -40.4008},
    {"filial_id": "0103X", "nome": "Sao Mateus",   "uf": "ES", "lat": -18.7204, "lon": -39.8589},
]

DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_hours,windspeed_10m_max"
BASE_URL_FORECAST = "https://api.open-meteo.com/v1/forecast"

PAST_DAYS = 92        # dias para trás (dado observado) -> necessário para chuva acumulada 30d
FORECAST_DAYS = 16    # dias para frente (previsão)

# Nomes das tabelas (camadas da arquitetura medalhão)
TB_BRONZE = "bronze_openmeteo_clima_raw"
TB_DIM    = "dim_localidade"
TB_FATO   = "fato_clima_diario"
TB_GOLD   = "gold_clima_filial"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 2 — TAREFA 0: PROVAR O BUG
# A pergunta é: existe mais de uma linha para a mesma filial no mesmo dia?
# Se existir, qualquer SUM no Power BI vai somar em dobro.
# =============================================================================
if spark.catalog.tableExists("silver_openmeteo_clima"):
    df_dup = (
        spark.table("silver_openmeteo_clima")
        .groupBy("filial_id", "data_referencia")   # o "grão" que deveria ser único
        .count()                                    # quantas linhas por grão
        .filter(F.col("count") > 1)                 # só os que estão repetidos
        .orderBy("filial_id", "data_referencia")
    )
    print(f"Pares duplicados na silver antiga: {df_dup.count()}")
    display(df_dup)
else:
    print("Tabela silver_openmeteo_clima não existe (ok, nada para provar).")

# Depois que a fato nova estiver validada, você pode apagar a tabela antiga:
# spark.sql("DROP TABLE IF EXISTS silver_openmeteo_clima")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 3 — TAREFA 1: FUNÇÃO DE CHAMADA À API (agora com dias passados)
# Mudança em relação à versão anterior: parâmetro "past_days".
# Com ele a API devolve, no MESMO array "daily", 30 dias passados + 16 futuros.
# =============================================================================
def chamar_open_meteo(filial: dict,
                      past_days: int = PAST_DAYS,
                      forecast_days: int = FORECAST_DAYS) -> dict:
    """Chama a Open-Meteo para uma filial e devolve o JSON bruto + metadados."""
    params = {
        "latitude": filial["lat"],
        "longitude": filial["lon"],
        "daily": DAILY_VARS,
        "timezone": "America/Sao_Paulo",   # garante que "dia" = dia de Brasília
        "past_days": past_days,
        "forecast_days": forecast_days,
    }

    resp = requests.get(BASE_URL_FORECAST, params=params, timeout=30)
    resp.raise_for_status()   # se vier 4xx/5xx, levanta exceção (tratada no loop)

    # Bronze = dado CRU. Guardamos o JSON inteiro como texto, sem transformar.
    # Se amanhã você precisar de outro campo, ele já está guardado aqui.
    return {
        "filial_id": filial["filial_id"],
        "raw_json": json.dumps(resp.json(), ensure_ascii=False),
        "status_code": resp.status_code,
        "url_chamada": resp.url,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 4 — TAREFA 1: INGESTÃO NA BRONZE
# A Bronze continua em modo APPEND: cada execução é um "snapshot" novo.
# Isso é proposital — a Bronze é o histórico de tudo que a API já respondeu.
# A deduplicação é responsabilidade da Silver, não da Bronze.
# =============================================================================
registros, erros = [], []

for filial in FILIAIS:
    try:
        registros.append(chamar_open_meteo(filial))
        print(f"OK   -> {filial['nome']} ({filial['filial_id']})")
    except Exception as e:
        # Uma filial com erro não derruba as outras
        erros.append({"filial_id": filial["filial_id"], "erro": str(e)})
        print(f"ERRO -> {filial['nome']}: {e}")

if registros:
    df_bronze_novo = (
        spark.createDataFrame([Row(**r) for r in registros])
        .withColumn("data_ingestao", F.current_date())           # usada na partição
        .withColumn("timestamp_ingestao", F.current_timestamp())  # usada no desempate e no watermark
        .withColumn("fonte", F.lit("open-meteo"))
    )

    # Conta ANTES de gravar. Contar depois re-executaria o plano inteiro
    # (e current_timestamp() geraria outro valor diferente do gravado).
    qtd = len(registros)

    (df_bronze_novo.write
        .format("delta")
        .mode("append")
        .partitionBy("data_ingestao")
        .saveAsTable(TB_BRONZE))

    print(f"\nBronze: +{qtd} linhas | Erros: {len(erros)}")
else:
    raise RuntimeError(f"Nenhuma filial ingerida. Erros: {erros}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 5 — TAREFA 2: DIMENSÃO DE LOCALIDADE
# Atributos que descrevem a filial (nome, UF, coordenadas) ficam AQUI,
# uma linha por filial. A fato guarda só o filial_id (chave).
# Por que overwrite aqui pode? Porque a dim é pequena e vem 100% do config:
# reconstruí-la inteira a cada execução é barato e sempre correto.
# =============================================================================
df_dim = spark.createDataFrame([
    Row(
        filial_id=f["filial_id"],
        filial_nome=f["nome"],
        uf=f["uf"],
        latitude=float(f["lat"]),
        longitude=float(f["lon"]),
    )
    for f in FILIAIS
])

(df_dim.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")   # permite mudar colunas da dim sem erro
    .saveAsTable(TB_DIM))

display(spark.table(TB_DIM))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 6 — TAREFA 3a: LER SÓ O QUE É NOVO NA BRONZE (watermark)
# Watermark = "marca d'água": o maior timestamp_ingestao que a fato já tem.
# Lemos da Bronze só o que veio DEPOIS dele -> processamento incremental.
#
# Por que timestamp e não data_ingestao? Se você rodar 2x no mesmo dia,
# a data é igual; com "data > watermark" a 2ª execução seria ignorada.
# O timestamp diferencia as duas.
# =============================================================================
fato_existe = spark.catalog.tableExists(TB_FATO)

if fato_existe:
    watermark = spark.table(TB_FATO).agg(F.max("timestamp_ingestao")).collect()[0][0]
else:
    watermark = None   # primeira carga: processa a Bronze inteira

print(f"Watermark atual: {watermark}")

df_bronze = spark.table(TB_BRONZE)
if watermark is not None:
    df_bronze = df_bronze.filter(F.col("timestamp_ingestao") > F.lit(watermark))

qtd_bronze_nova = df_bronze.count()
print(f"Linhas novas na Bronze para processar: {qtd_bronze_nova}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 7 — TAREFA 3b: PARSE, EXPLODE, CLASSIFICAÇÃO E DEDUPLICAÇÃO
# =============================================================================

# 1) Schema só do bloco "daily". Declarar o schema é mais rápido e mais seguro
#    do que deixar o Spark inferir (inferência lê os dados duas vezes).
daily_schema = StructType([
    StructField("daily", StructType([
        StructField("time",                ArrayType(StringType())),
        StructField("temperature_2m_max",  ArrayType(DoubleType())),
        StructField("temperature_2m_min",  ArrayType(DoubleType())),
        StructField("precipitation_sum",   ArrayType(DoubleType())),
        StructField("precipitation_hours", ArrayType(DoubleType())),
        StructField("windspeed_10m_max",   ArrayType(DoubleType())),
    ]))
])

# 2) A API devolve ARRAYS PARALELOS: time[0] combina com temp_max[0], etc.
#    arrays_zip junta posição a posição em um array de structs;
#    explode transforma cada elemento do array em uma linha.
df_dias = (
    df_bronze
    .withColumn("j", F.from_json("raw_json", daily_schema))
    .withColumn("dia", F.explode(F.arrays_zip(
        F.col("j.daily.time").alias("data"),
        F.col("j.daily.temperature_2m_max").alias("temperatura_max"),
        F.col("j.daily.temperature_2m_min").alias("temperatura_min"),
        F.col("j.daily.precipitation_sum").alias("precipitacao_mm"),
        F.col("j.daily.precipitation_hours").alias("horas_chuva"),
        F.col("j.daily.windspeed_10m_max").alias("vento_max_kmh"),
    )))
    .select(
        "filial_id",
        F.to_date("dia.data").alias("data_referencia"),
        F.col("dia.temperatura_max").alias("temperatura_max"),
        F.col("dia.temperatura_min").alias("temperatura_min"),
        F.col("dia.precipitacao_mm").alias("precipitacao_mm"),
        F.col("dia.horas_chuva").alias("horas_chuva"),
        F.col("dia.vento_max_kmh").alias("vento_max_kmh"),
        "data_ingestao",
        "timestamp_ingestao",
        "fonte",
    )
)

# 3) Classificação: dia anterior à ingestão já aconteceu (observado);
#    o próprio dia da ingestão e os seguintes ainda são previsão.
df_dias = df_dias.withColumn(
    "tipo",
    F.when(F.col("data_referencia") < F.col("data_ingestao"), F.lit("observado"))
     .otherwise(F.lit("previsao"))
)

# 4) DEDUPLICAÇÃO — o coração da correção do bug.
#    Se no lote houver várias ingestões, o mesmo (filial, dia) aparece várias
#    vezes com valores DIFERENTES (a previsão muda a cada dia).
#    Regra: vale a ingestão mais recente, pois é a informação mais atualizada
#    (e, para dias passados, é o dado observado em vez da antiga previsão).
#
#    Window = "grupo de linhas" sem colapsar o DataFrame (diferente do groupBy).
#    row_number numera as linhas de cada grupo na ordem pedida: 1 = mais recente.
w_ultima = (
    Window
    .partitionBy("filial_id", "data_referencia")
    .orderBy(F.col("timestamp_ingestao").desc())
)

df_fato_lote = (
    df_dias
    .withColumn("rn", F.row_number().over(w_ultima))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 8 — TAREFA 4: CHECAGENS DE QUALIDADE
# from_json NÃO dá erro se o JSON vier diferente: devolve null em silêncio.
# Por isso validamos explicitamente e PARAMOS o pipeline se algo estiver errado.
# Melhor falhar alto do que gravar lixo que ninguém percebe.
# =============================================================================
def validar_qualidade(df):
    """Levanta ValueError se o DataFrame violar alguma regra da fato."""
    problemas = []

    # Regra 1: chave nunca nula
    nulos = df.filter(F.col("data_referencia").isNull() | F.col("filial_id").isNull()).count()
    if nulos > 0:
        problemas.append(f"{nulos} linha(s) com chave nula")

    # Regra 2: grão único (1 linha por filial + dia)
    dups = df.groupBy("filial_id", "data_referencia").count().filter("count > 1").count()
    if dups > 0:
        problemas.append(f"{dups} par(es) filial/dia duplicado(s)")

    # Regra 3: coerência física. Linhas com null não entram aqui
    # (comparação com null resulta em null, e o filter descarta).
    invertidas = df.filter(F.col("temperatura_min") > F.col("temperatura_max")).count()
    if invertidas > 0:
        problemas.append(f"{invertidas} linha(s) com temperatura_min > temperatura_max")

    if problemas:
        raise ValueError("Falha de qualidade: " + " | ".join(problemas))

    print("Qualidade OK ✔")

# Cache: o df_fato_lote vai ser usado várias vezes (checagens + merge).
# Sem cache, o Spark recalcularia parse + explode + window a cada ação.
df_fato_lote = df_fato_lote.cache()

if qtd_bronze_nova > 0:
    validar_qualidade(df_fato_lote)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 9 — TAREFA 3c: GRAVAR NA FATO COM MERGE (upsert)
# MERGE compara origem (s) e destino (t) pela chave:
#   - existe no destino  -> UPDATE (atualiza a previsão / vira observado)
#   - não existe         -> INSERT (dia novo)
# Rodar de novo com os mesmos dados não cria linha nova => IDEMPOTENTE.
# =============================================================================
if qtd_bronze_nova == 0:
    print("Nada novo na Bronze. Fato inalterada.")

elif not fato_existe:
    # Primeira carga: a tabela ainda não existe, então só criamos.
    (df_fato_lote.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TB_FATO))
    print(f"Fato criada com {spark.table(TB_FATO).count()} linhas.")

else:
    fato = DeltaTable.forName(spark, TB_FATO)
    (
        fato.alias("t")
        .merge(
            df_fato_lote.alias("s"),
            "t.filial_id = s.filial_id AND t.data_referencia = s.data_referencia"
        )
        # A condição extra protege contra "voltar no tempo": se alguém
        # reprocessar uma ingestão antiga, ela NÃO sobrescreve uma mais nova.
        .whenMatchedUpdateAll(condition="s.timestamp_ingestao > t.timestamp_ingestao")
        .whenNotMatchedInsertAll()
        .execute()
    )

    # O Delta registra o que o MERGE fez em cada versão da tabela.
    metricas = fato.history(1).select("operationMetrics").collect()[0][0]
    print("MERGE concluído:")
    print(f"  inseridas:   {metricas.get('numTargetRowsInserted')}")
    print(f"  atualizadas: {metricas.get('numTargetRowsUpdated')}")
    print(f"  total fato:  {spark.table(TB_FATO).count()}")

df_fato_lote.unpersist()   # libera a memória do cache

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 10 — VALIDAÇÃO DOS CRITÉRIOS DE ACEITE
# Rode o notebook inteiro 3 vezes: a contagem por tipo deve ficar igual
# (só muda se virar o dia, porque a janela de 30+16 dias anda junto).
# =============================================================================
df_fato = spark.table(TB_FATO)

dup_fato = df_fato.groupBy("filial_id", "data_referencia").count().filter("count > 1").count()
print(f"Duplicados na fato (esperado 0): {dup_fato}")

display(
    df_fato.groupBy("filial_id", "tipo")
    .agg(
        F.count("*").alias("dias"),
        F.min("data_referencia").alias("de"),
        F.max("data_referencia").alias("ate"),
    )
    .orderBy("filial_id", "tipo")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 11 — TAREFA 5: CAMADA GOLD
# Gold = tabela pronta para o Power BI, já com as regras de negócio aplicadas.
# Overwrite aqui é ok: a Gold é 100% derivada da fato e é pequena.
# =============================================================================

# Janela móvel por filial, ordenada por dia.
# rowsBetween(-6, 0) = "as 6 linhas anteriores + a atual" = 7 dias.
# Atenção: isso conta LINHAS, não dias. Funciona porque a API não pula dias.
# Se houvesse buracos, o certo seria rangeBetween sobre um número de dia.
w_filial = Window.partitionBy("filial_id").orderBy("data_referencia")

df_gold = (
    spark.table(TB_FATO)
    .withColumn("chuva_acum_7d",  F.sum("precipitacao_mm").over(w_filial.rowsBetween(-6, 0)))
    .withColumn("chuva_acum_30d", F.sum("precipitacao_mm").over(w_filial.rowsBetween(-29, 0)))
    # Regra de negócio: dá para mandar técnico ao campo?
    # coalesce(..., 0) evita que um null deixe a flag nula.
    .withColumn(
        "dia_favoravel_campo",
        (F.coalesce(F.col("precipitacao_mm"), F.lit(0)) < 5) &
        (F.coalesce(F.col("vento_max_kmh"),  F.lit(0)) < 30)
    )
    # Join com a dimensão só para trazer o nome (facilita leitura).
    # A dim é minúscula -> broadcast evita shuffle da tabela grande.
    .join(F.broadcast(spark.table(TB_DIM).select("filial_id", "filial_nome")),
          on="filial_id", how="left")
    .select(
        "filial_id", "filial_nome", "data_referencia", "tipo",
        "temperatura_max", "temperatura_min", "precipitacao_mm", "vento_max_kmh",
        F.round("chuva_acum_7d", 1).alias("chuva_acum_7d"),
        F.round("chuva_acum_30d", 1).alias("chuva_acum_30d"),
        "dia_favoravel_campo",
    )
)

(df_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TB_GOLD))

display(spark.table(TB_GOLD).orderBy("filial_id", "data_referencia"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 12 — TESTE: A CHECAGEM DE QUALIDADE REALMENTE FUNCIONA?
# Criamos uma cópia "corrompida" da fato (sem gravar nada) e conferimos
# que validar_qualidade() levanta erro. Um teste que nunca falha não testa nada.
# =============================================================================
df_teste = spark.table(TB_FATO).limit(5)

# Corrompe: inverte min/max na primeira linha e duplica uma linha
primeira = df_teste.limit(1)
df_corrompido = (
    df_teste
    .unionByName(primeira)                                   # gera duplicidade
    .withColumn("temperatura_min", F.col("temperatura_max") + 10)  # min > max
)

try:
    validar_qualidade(df_corrompido)
    print("❌ O teste NÃO pegou o problema — revise validar_qualidade()")
except ValueError as e:
    print(f"✔ Checagem funcionou e bloqueou: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 13 — AUDITORIA DO PIPELINE
# Cada checagem compara um valor ESPERADO com o OBTIDO.
# Ao final, mostra uma tabela com ✔ (passou) ou ❌ (falhou).
# =============================================================================
from pyspark.sql import Window
from pyspark.sql import functions as F

resultados = []   # lista de (checagem, esperado, obtido)

def checar(nome, esperado, obtido):
    resultados.append((nome, str(esperado), str(obtido),
                       "✔" if esperado == obtido else "❌"))

df_bronze = spark.table(TB_BRONZE)
df_dim    = spark.table(TB_DIM)
df_fato   = spark.table(TB_FATO)
df_gold   = spark.table(TB_GOLD)

# --- 1. Dimensão: 1 linha por filial do config ------------------------------
checar("dim: qtde de filiais = config", len(FILIAIS), df_dim.count())
checar("dim: filial_id único", df_dim.count(),
       df_dim.select("filial_id").distinct().count())

# --- 2. Fato: grão único (o bug original) -----------------------------------
dups = df_fato.groupBy("filial_id", "data_referencia").count().filter("count > 1").count()
checar("fato: pares filial/dia duplicados", 0, dups)

# --- 3. Fato: chaves nunca nulas --------------------------------------------
nulos = df_fato.filter(F.col("filial_id").isNull() | F.col("data_referencia").isNull()).count()
checar("fato: chaves nulas", 0, nulos)

# --- 4. Fato: sem buraco de datas por filial --------------------------------
# Se não há buraco: qtde de dias = (última data - primeira data) + 1
buracos = (
    df_fato.groupBy("filial_id")
    .agg(F.count("*").alias("dias"),
         (F.datediff(F.max("data_referencia"), F.min("data_referencia")) + 1).alias("esperado"))
    .filter("dias <> esperado")
    .count()
)
checar("fato: filiais com buraco de datas", 0, buracos)

# --- 5. Fato: toda filial existe na dimensão (sem órfãos) -------------------
orfaos = df_fato.join(df_dim, "filial_id", "left_anti").count()
checar("fato: filial_id órfão (sem dim)", 0, orfaos)

# --- 6. Fato: classificação 'tipo' coerente ---------------------------------
tipo_errado = df_fato.filter(
    ((F.col("tipo") == "observado") & (F.col("data_referencia") >= F.col("data_ingestao"))) |
    ((F.col("tipo") == "previsao")  & (F.col("data_referencia") <  F.col("data_ingestao")))
).count()
checar("fato: 'tipo' incoerente com a data", 0, tipo_errado)

# --- 7. Fato: tem a ingestão mais recente da Bronze (watermark em dia) ------
max_bronze = df_bronze.agg(F.max("timestamp_ingestao")).collect()[0][0]
max_fato   = df_fato.agg(F.max("timestamp_ingestao")).collect()[0][0]
checar("fato: última ingestão = última da Bronze", max_bronze, max_fato)

# --- 8. Fato: hoje deve vir da ingestão mais recente (regra do desempate) ---
# Pega a linha de HOJE de cada filial e confere que veio do último snapshot.
hoje_desatualizado = (
    df_fato.filter(F.col("data_referencia") == F.current_date())
    .filter(F.col("timestamp_ingestao") < F.lit(max_bronze))
    .count()
)
checar("fato: dia de hoje vindo de snapshot antigo", 0, hoje_desatualizado)

# --- 9. Gold: mesma quantidade de linhas da fato ----------------------------
checar("gold: linhas = fato", df_fato.count(), df_gold.count())

# --- 10. Gold: recalcula chuva 7d "na mão" e compara ------------------------
# Escolhe uma filial e a última data observada; soma a chuva dos 7 dias
# terminando nela com um filtro simples (sem window) e compara com a Gold.
filial_teste = FILIAIS[0]["filial_id"]
data_teste = (df_fato.filter((F.col("filial_id") == filial_teste) & (F.col("tipo") == "observado"))
              .agg(F.max("data_referencia")).collect()[0][0])

manual = (df_fato.filter((F.col("filial_id") == filial_teste) &
                         F.col("data_referencia").between(F.date_sub(F.lit(data_teste), 6), F.lit(data_teste)))
          .agg(F.round(F.sum("precipitacao_mm"), 1)).collect()[0][0])

na_gold = (df_gold.filter((F.col("filial_id") == filial_teste) &
                          (F.col("data_referencia") == F.lit(data_teste)))
           .select("chuva_acum_7d").collect()[0][0])
checar(f"gold: chuva 7d manual ({filial_teste} {data_teste})", manual, na_gold)

# --- Resultado ---------------------------------------------------------------
df_auditoria = spark.createDataFrame(resultados, ["checagem", "esperado", "obtido", "status"])
display(df_auditoria)

falhas = df_auditoria.filter("status = '❌'").count()
print("✔ TUDO OK" if falhas == 0 else f"❌ {falhas} checagem(ns) falharam — revise acima")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 14 — DIMENSÃO CALENDÁRIO
# Por que no Spark e não no Power BI? Tabelas Direct Lake são lidas direto
# do Delta. Não dá para criar coluna calculada nelas, e uma calendário em DAX
# não tem as colunas de negócio. Então materializamos tudo aqui.
# =============================================================================
TB_CAL = "dim_calendario"

# Intervalo: da menor à maior data da fato
limites = spark.table(TB_FATO).agg(
    F.min("data_referencia").alias("ini"),
    F.max("data_referencia").alias("fim"),
).collect()[0]

# sequence() gera um array com todas as datas do intervalo;
# explode() transforma o array em uma linha por data.
df_cal = spark.sql(f"""
    SELECT explode(sequence(DATE'{limites.ini}', DATE'{limites.fim}', INTERVAL 1 DAY)) AS data
""")

# Nomes em português via array indexado pelo nº do mês/dia (o array começa em 1)
MESES = ["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"]
DIAS  = ["Dom","Seg","Ter","Qua","Qui","Sex","Sáb"]   # dayofweek: 1 = domingo

df_cal = (
    df_cal
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("nome_mes", F.element_at(F.array(*[F.lit(m) for m in MESES]), F.col("mes")))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("dia_semana_num", F.dayofweek("data"))
    .withColumn("nome_dia_semana", F.element_at(F.array(*[F.lit(d) for d in DIAS]), F.col("dia_semana_num")))
    .withColumn("fim_de_semana", F.col("dia_semana_num").isin(1, 7))
    # Ano fiscal: novembro a outubro. Nov/2026 pertence ao FY27.
    .withColumn("ano_fiscal",
        F.when(F.col("mes") >= 11, F.col("ano") + 1).otherwise(F.col("ano")))
    .withColumn("fy", F.concat(F.lit("FY"), F.substring(F.col("ano_fiscal").cast("string"), 3, 2)))
    # Ordem do mês dentro do FY (Nov = 1 ... Out = 12), para ordenar gráficos
    .withColumn("mes_fy_ordem", ((F.col("mes") + 1) % 12) + 1)
)

(df_cal.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(TB_CAL))

display(spark.table(TB_CAL).orderBy("data"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 15 — OTIMIZAÇÃO PARA DIRECT LAKE
# O Direct Lake carrega as colunas do Parquet direto para a memória.
# - VORDER: ordenação/compressão do Fabric que deixa essa leitura bem mais rápida
# - OPTIMIZE: junta arquivos pequenos (cada MERGE gera alguns arquivos novos;
#   arquivos demais = mais lentidão e risco de fallback para DirectQuery)
# =============================================================================
for tabela in [TB_GOLD, TB_DIM, TB_CAL]:
    spark.sql(f"OPTIMIZE {tabela} VORDER")
    print(f"Otimizada: {tabela}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Para cada filial, compara cada data com a anterior (lag).
# Se a diferença for maior que 1 dia, existe um buraco entre elas.
w = Window.partitionBy("filial_id").orderBy("data_referencia")

display(
    spark.table(TB_FATO)
    .withColumn("data_anterior", F.lag("data_referencia").over(w))
    .withColumn("dias_sem_dado", F.datediff("data_referencia", "data_anterior") - 1)
    .filter("dias_sem_dado > 0")
    .select("filial_id", "data_anterior", "data_referencia", "dias_sem_dado")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# CÉLULA 16 — ATUALIZAR O MODELO SEMÂNTICO (Direct Lake)
# Depois de gravar as tabelas Delta, pede ao modelo SM_Clima_Filiais que passe a
# ler a versão nova (reframing). Garante que o relatório mostre os dados do dia
# logo após a execução agendada, sem depender da atualização automática.
# =============================================================================
import sempy.fabric as fabric

fabric.refresh_dataset(dataset="SM_Clima_Filiais")
print("Modelo SM_Clima_Filiais atualizado ✔")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
