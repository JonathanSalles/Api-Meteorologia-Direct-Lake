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

# CELL ********************

import requests
import json
from datetime import datetime, date
from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp, current_date
from pyspark.sql.functions import from_json, col, explode, to_date, arrays_zip

# Ajuste as coordenadas para as filiais reais
# (lat/lon aproximados - troque pelos exatos se quiser mais precisão)
FILIAIS = [
    {"filial_id": "0101X", "nome": "Linhares",     "uf": "ES", "lat": -19.4021, "lon": -40.0729},
    {"filial_id": "020X",  "nome": "Colatina",     "uf": "ES", "lat": -19.5386, "lon": -40.6306},
    {"filial_id": "0102X", "nome": "Nova Venecia",  "uf": "ES", "lat": -18.7139, "lon": -40.4008},
    {"filial_id": "0103X", "nome": "Sao Mateus",    "uf": "ES", "lat": -18.7204, "lon": -39.8589},
]

DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum,precipitation_hours,windspeed_10m_max"
BASE_URL_FORECAST = "https://api.open-meteo.com/v1/forecast"

LAKEHOUSE_BRONZE_PATH = "Tables/bronze_openmeteo_clima_raw"  # ajuste pro nome do seu lakehouse/schema


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def chamar_open_meteo(filial: dict, forecast_days: int = 16) -> dict:
    """Chama a API Open-Meteo (forecast) para uma filial e retorna o JSON bruto + metadados."""
    params = {
        "latitude": filial["lat"],
        "longitude": filial["lon"],
        "daily": DAILY_VARS,
        "timezone": "America/Sao_Paulo",
        "forecast_days": forecast_days,
    }

    resp = requests.get(BASE_URL_FORECAST, params=params, timeout=30)
    resp.raise_for_status()

    return {
        "filial_id": filial["filial_id"],
        "filial_nome": filial["nome"],
        "uf": filial["uf"],
        "latitude": filial["lat"],
        "longitude": filial["lon"],
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

registros = []
erros = []

for filial in FILIAIS:
    try:
        registro = chamar_open_meteo(filial)
        registros.append(registro)
        print(f"OK  -> {filial['nome']} ({filial['filial_id']})")
    except Exception as e:
        erros.append({"filial_id": filial["filial_id"], "erro": str(e)})
        print(f"ERRO -> {filial['nome']}: {e}")

print(f"\nTotal ingerido: {len(registros)} | Erros: {len(erros)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if registros:
    df_bronze = spark.createDataFrame([Row(**r) for r in registros])

    df_bronze = (
        df_bronze
        .withColumn("data_ingestao", current_date())
        .withColumn("timestamp_ingestao", current_timestamp())
        .withColumn("fonte", lit("open-meteo"))
    )

    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .partitionBy("data_ingestao")
        .saveAsTable("bronze_openmeteo_clima_raw")   # cria/anexa tabela gerenciada no Lakehouse
    )

    print(f"Gravado com sucesso: {df_bronze.count()} linhas em bronze_openmeteo_clima_raw")
    display(df_bronze)
else:
    print("Nenhum registro para gravar - verifique os erros acima.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_Teste.dbo.bronze_openmeteo_clima_raw LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from pyspark.sql.functions import from_json, col, explode, to_date

# Schema só do bloco "daily" do retorno da Open-Meteo (o resto do JSON é ignorado)
daily_schema = StructType([
    StructField("daily", StructType([
        StructField("time", ArrayType(StringType())),
        StructField("temperature_2m_max", ArrayType(DoubleType())),
        StructField("temperature_2m_min", ArrayType(DoubleType())),
        StructField("precipitation_sum", ArrayType(DoubleType())),
        StructField("precipitation_hours", ArrayType(DoubleType())),
        StructField("windspeed_10m_max", ArrayType(DoubleType())),
    ]))
])

# Lê a bronze (raw_json ainda como texto)
df_bronze = spark.sql("SELECT * FROM LH_Teste.dbo.bronze_openmeteo_clima_raw")

# Faz o parse do JSON e junta os arrays paralelos (time, temp_max, temp_min...) em um único array de structs
df_zip = (
    df_bronze
    .withColumn("json_parsed", from_json(col("raw_json"), daily_schema))
    .withColumn("dia_zip", arrays_zip(
        col("json_parsed.daily.time").alias("data"),
        col("json_parsed.daily.temperature_2m_max").alias("temperatura_max"),
        col("json_parsed.daily.temperature_2m_min").alias("temperatura_min"),
        col("json_parsed.daily.precipitation_sum").alias("precipitacao_mm"),
        col("json_parsed.daily.precipitation_hours").alias("horas_chuva"),
        col("json_parsed.daily.windspeed_10m_max").alias("vento_max_kmh"),
    ))
)

# Explode: transforma o array de 16 dias em 16 linhas (uma por dia por filial)
df_exploded = df_zip.withColumn("dia", explode(col("dia_zip")))

# Seleciona e tipa as colunas finais
df_silver = df_exploded.select(
    col("filial_id"),
    col("filial_nome"),
    col("uf"),
    col("latitude"),
    col("longitude"),
    to_date(col("dia.data")).alias("data_referencia"),
    col("dia.temperatura_max").alias("temperatura_max"),
    col("dia.temperatura_min").alias("temperatura_min"),
    col("dia.precipitacao_mm").alias("precipitacao_mm"),
    col("dia.horas_chuva").alias("horas_chuva"),
    col("dia.vento_max_kmh").alias("vento_max_kmh"),
    col("data_ingestao"),
    col("fonte"),
)

# Grava como tabela silver (a bronze com raw_json continua intacta, para auditoria)
(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("silver_openmeteo_clima")
)

print(f"Gravado com sucesso: {df_silver.count()} linhas em silver_openmeteo_clima")
display(df_silver)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
