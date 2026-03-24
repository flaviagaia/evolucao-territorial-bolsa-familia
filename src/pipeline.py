from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd

try:
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    PYSPARK_AVAILABLE = True
except ModuleNotFoundError:
    SparkSession = Any  # type: ignore[assignment]
    Window = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    PYSPARK_AVAILABLE = False

from .config import (
    ANOMALY_METRICS_PATH,
    MUNICIPAL_TIMESERIES_PATH,
    OPERATIONAL_METRICS_PATH,
    PROCESSED_DIR,
    RAW_DATA_PATH,
    REGRESSION_METRICS_PATH,
    SUMMARY_PATH,
    TERRITORIAL_METRICS_PATH,
    CLUSTERING_METRICS_PATH,
)
from .ml_models import run_ml_models


def create_spark_session(app_name: str = "bolsa-familia-territorial-panel") -> SparkSession:
    if not PYSPARK_AVAILABLE:
        raise ModuleNotFoundError("pyspark is not installed in this environment")
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def _build_territorial_metrics_pandas() -> pd.DataFrame:
    raw_df = pd.read_csv(RAW_DATA_PATH)
    raw_df = raw_df[raw_df["social_categoria"] == "Bolsa Família"].copy()
    pivot_df = (
        raw_df.pivot_table(
            index=["co_mun", "no_mun", "ano"],
            columns="social_subcategoria",
            values="valor",
            aggfunc="first",
        )
        .reset_index()
        .rename(
            columns={
                "co_mun": "codigo_municipio",
                "no_mun": "municipio",
                "Valor Total Repassado do Bolsa Família": "valor_total_repassado",
                "Famílias beneficiárias": "familias_beneficiarias",
                "Benefício médio recebido pelas famílias do Bolsa Família": "beneficio_medio",
            }
        )
    )
    pivot_df["ano"] = pivot_df["ano"].astype(int)
    pivot_df = pivot_df.sort_values(["codigo_municipio", "ano"])
    pivot_df["crescimento_valor_pct"] = (
        pivot_df.groupby("codigo_municipio")["valor_total_repassado"].pct_change() * 100
    ).round(2)
    pivot_df["crescimento_familias_pct"] = (
        pivot_df.groupby("codigo_municipio")["familias_beneficiarias"].pct_change() * 100
    ).round(2)
    return pivot_df


def _build_operational_metrics_pandas(territorial_df: pd.DataFrame) -> pd.DataFrame:
    base = territorial_df[territorial_df["ano"] >= 2021].copy()
    months = pd.DataFrame({"mes": list(range(1, 13))})
    base["key"] = 1
    months["key"] = 1
    monthly_df = base.merge(months, on="key").drop(columns=["key"])
    monthly_df["sazonalidade"] = monthly_df["mes"].map(
        lambda month: 0.96 if month in (1, 2) else 1.05 if month in (11, 12) else 1.0
    )
    monthly_df["valor_pago_estimado"] = (
        (monthly_df["valor_total_repassado"] / 12.0) * monthly_df["sazonalidade"]
    ).round(2)
    monthly_df["familias_mes_estimadas"] = (
        monthly_df["familias_beneficiarias"] * monthly_df["sazonalidade"]
    ).round(0)
    monthly_df["fator_saque"] = (
        0.91 + (monthly_df["mes"] / 100.0) + ((monthly_df["codigo_municipio"] % 7) / 100.0)
    ).round(4)
    monthly_df["fator_saque"] = monthly_df["fator_saque"].clip(upper=1.0)
    monthly_df["valor_sacado_estimado"] = (
        monthly_df["valor_pago_estimado"] * monthly_df["fator_saque"]
    ).round(2)
    monthly_df["taxa_saque_pct"] = (
        (monthly_df["valor_sacado_estimado"] / monthly_df["valor_pago_estimado"]) * 100
    ).round(2)
    monthly_df["gap_pagamento_saque"] = (
        monthly_df["valor_pago_estimado"] - monthly_df["valor_sacado_estimado"]
    ).round(2)
    monthly_df["risco_operacional"] = monthly_df["taxa_saque_pct"].map(
        lambda value: "alto" if value < 92 else "moderado" if value < 96 else "baixo"
    )
    return monthly_df


def _build_summary_pandas(territorial_df: pd.DataFrame, operational_df: pd.DataFrame) -> dict[str, float]:
    latest_year = int(territorial_df["ano"].max())
    top_city = (
        territorial_df[territorial_df["ano"] == latest_year]
        .sort_values("valor_total_repassado", ascending=False)
        .iloc[0]
    )
    return {
        "anos_cobertos": int(territorial_df["ano"].nunique()),
        "municipios_cobertos": int(territorial_df["codigo_municipio"].nunique()),
        "ano_mais_recente": latest_year,
        "maior_municipio_ano_recente": str(top_city["municipio"]),
        "maior_repasse_ano_recente": float(top_city["valor_total_repassado"]),
        "linhas_operacionais": int(len(operational_df)),
        "taxa_media_saque_pct": float(round(operational_df["taxa_saque_pct"].mean(), 2)),
        "engine": "pandas_fallback",
    }


def _save_outputs_pandas(territorial_df: pd.DataFrame, operational_df: pd.DataFrame, summary: dict[str, float]) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    territorial_df.to_parquet(TERRITORIAL_METRICS_PATH, index=False)
    operational_df.to_parquet(OPERATIONAL_METRICS_PATH, index=False)
    operational_df[
        [
            "codigo_municipio",
            "municipio",
            "ano",
            "mes",
            "valor_pago_estimado",
            "valor_sacado_estimado",
            "gap_pagamento_saque",
            "taxa_saque_pct",
            "risco_operacional",
        ]
    ].to_parquet(MUNICIPAL_TIMESERIES_PATH, index=False)
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False))


def run_pipeline_pandas() -> dict[str, float]:
    territorial_df = _build_territorial_metrics_pandas()
    operational_df = _build_operational_metrics_pandas(territorial_df)
    summary = _build_summary_pandas(territorial_df, operational_df)
    _save_outputs_pandas(territorial_df, operational_df, summary)
    ml_metrics = run_ml_models()
    summary.update(
        {
            "regression_r2": ml_metrics["regression"]["r2"],
            "clusters": ml_metrics["clustering"]["clusters"],
            "silhouette_score": ml_metrics["clustering"]["silhouette_score"],
            "anomaly_rows": ml_metrics["anomaly_detection"]["anomaly_rows"],
        }
    )
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


def _load_raw_dataset(spark: SparkSession):
    if not PYSPARK_AVAILABLE or F is None:
        raise ModuleNotFoundError("pyspark is not installed in this environment")
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(RAW_DATA_PATH))
        .withColumnRenamed("co_mun", "codigo_municipio")
        .withColumnRenamed("no_mun", "municipio")
        .withColumnRenamed("ano", "ano")
        .withColumnRenamed("social_subcategoria", "subcategoria")
        .withColumn("valor", F.col("valor").cast("double"))
        .filter(F.col("social_categoria") == "Bolsa Família")
    )


def build_territorial_metrics(spark: SparkSession):
    if not PYSPARK_AVAILABLE or Window is None or F is None:
        raise ModuleNotFoundError("pyspark is not installed in this environment")
    raw_df = _load_raw_dataset(spark)

    pivot_df = (
        raw_df.groupBy("codigo_municipio", "municipio", "ano")
        .pivot(
            "subcategoria",
            [
                "Valor Total Repassado do Bolsa Família",
                "Famílias beneficiárias",
                "Benefício médio recebido pelas famílias do Bolsa Família",
            ],
        )
        .agg(F.first("valor"))
        .withColumnRenamed(
            "Valor Total Repassado do Bolsa Família",
            "valor_total_repassado",
        )
        .withColumnRenamed("Famílias beneficiárias", "familias_beneficiarias")
        .withColumnRenamed(
            "Benefício médio recebido pelas famílias do Bolsa Família",
            "beneficio_medio",
        )
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("familias_beneficiarias", F.col("familias_beneficiarias").cast("double"))
        .withColumn("valor_total_repassado", F.col("valor_total_repassado").cast("double"))
        .withColumn("beneficio_medio", F.col("beneficio_medio").cast("double"))
    )

    growth_window = Window.partitionBy("codigo_municipio").orderBy("ano")
    territorial_df = (
        pivot_df.withColumn(
            "crescimento_valor_pct",
            F.round(
                (
                    (F.col("valor_total_repassado") - F.lag("valor_total_repassado").over(growth_window))
                    / F.lag("valor_total_repassado").over(growth_window)
                )
                * 100,
                2,
            ),
        )
        .withColumn(
            "crescimento_familias_pct",
            F.round(
                (
                    (F.col("familias_beneficiarias") - F.lag("familias_beneficiarias").over(growth_window))
                    / F.lag("familias_beneficiarias").over(growth_window)
                )
                * 100,
                2,
            ),
        )
    )
    return territorial_df


def build_operational_metrics(territorial_df):
    if not PYSPARK_AVAILABLE or F is None:
        raise ModuleNotFoundError("pyspark is not installed in this environment")
    spark = territorial_df.sparkSession
    monthly_df = (
        territorial_df.filter(F.col("ano") >= 2021)
        .crossJoin(
            spark.range(1, 13).toDF("mes").withColumn("mes", F.col("mes").cast("int"))
        )
        .withColumn(
            "sazonalidade",
            F.when(F.col("mes").isin(1, 2), F.lit(0.96))
            .when(F.col("mes").isin(11, 12), F.lit(1.05))
            .otherwise(F.lit(1.00)),
        )
        .withColumn(
            "valor_pago_estimado",
            F.round((F.col("valor_total_repassado") / F.lit(12.0)) * F.col("sazonalidade"), 2),
        )
        .withColumn(
            "familias_mes_estimadas",
            F.round(F.col("familias_beneficiarias") * F.col("sazonalidade"), 0),
        )
        .withColumn(
            "fator_saque",
            F.round(
                F.lit(0.91)
                + (F.col("mes") / F.lit(100.0))
                + ((F.col("codigo_municipio") % 7) / F.lit(100.0)),
                4,
            ),
        )
        .withColumn(
            "valor_sacado_estimado",
            F.round(F.col("valor_pago_estimado") * F.least(F.col("fator_saque"), F.lit(1.0)), 2),
        )
        .withColumn(
            "taxa_saque_pct",
            F.round((F.col("valor_sacado_estimado") / F.col("valor_pago_estimado")) * 100, 2),
        )
        .withColumn(
            "gap_pagamento_saque",
            F.round(F.col("valor_pago_estimado") - F.col("valor_sacado_estimado"), 2),
        )
        .withColumn(
            "risco_operacional",
            F.when(F.col("taxa_saque_pct") < 92, F.lit("alto"))
            .when(F.col("taxa_saque_pct") < 96, F.lit("moderado"))
            .otherwise(F.lit("baixo")),
        )
    )
    return monthly_df


def build_summary(territorial_df, operational_df) -> dict[str, float]:
    latest_year = territorial_df.agg(F.max("ano").alias("ano")).collect()[0]["ano"]
    top_city = (
        territorial_df.filter(F.col("ano") == latest_year)
        .orderBy(F.desc("valor_total_repassado"))
        .select("municipio", "valor_total_repassado")
        .first()
    )
    summary = {
        "anos_cobertos": int(territorial_df.select("ano").distinct().count()),
        "municipios_cobertos": int(territorial_df.select("codigo_municipio").distinct().count()),
        "ano_mais_recente": int(latest_year),
        "maior_municipio_ano_recente": top_city["municipio"],
        "maior_repasse_ano_recente": float(top_city["valor_total_repassado"]),
        "linhas_operacionais": int(operational_df.count()),
        "taxa_media_saque_pct": float(
            operational_df.agg(F.round(F.avg("taxa_saque_pct"), 2).alias("media")).collect()[0]["media"]
        ),
    }
    return summary


def save_outputs(territorial_df, operational_df, summary: dict[str, float]) -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    territorial_df.write.mode("overwrite").parquet(str(TERRITORIAL_METRICS_PATH))
    operational_df.write.mode("overwrite").parquet(str(OPERATIONAL_METRICS_PATH))

    (
        operational_df.select(
            "codigo_municipio",
            "municipio",
            "ano",
            "mes",
            "valor_pago_estimado",
            "valor_sacado_estimado",
            "gap_pagamento_saque",
            "taxa_saque_pct",
            "risco_operacional",
        )
        .write.mode("overwrite")
        .parquet(str(MUNICIPAL_TIMESERIES_PATH))
    )

    SUMMARY_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False))


def run_pipeline() -> dict[str, float]:
    if os.getenv("USE_PYSPARK", "0") != "1":
        return run_pipeline_pandas()

    if shutil.which("java") is None:
        return run_pipeline_pandas()

    try:
        spark = create_spark_session()
    except Exception:
        return run_pipeline_pandas()

    try:
        territorial_df = build_territorial_metrics(spark)
        operational_df = build_operational_metrics(territorial_df)
        summary = build_summary(territorial_df, operational_df)
        summary["engine"] = "pyspark"
        save_outputs(territorial_df, operational_df, summary)
        ml_metrics = run_ml_models()
        summary.update(
            {
                "regression_r2": ml_metrics["regression"]["r2"],
                "clusters": ml_metrics["clustering"]["clusters"],
                "silhouette_score": ml_metrics["clustering"]["silhouette_score"],
                "anomaly_rows": ml_metrics["anomaly_detection"]["anomaly_rows"],
            }
        )
        SUMMARY_PATH.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
        return summary
    finally:
        spark.stop()
