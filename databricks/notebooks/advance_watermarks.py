# Databricks notebook source
"""Advance source watermarks after a successful dbt build."""

# COMMAND ----------

parameter_names = [
    "pipeline_name",
    "sales_watermark_start",
    "sales_watermark_end",
    "customer_watermark_start",
    "customer_watermark_end",
    "product_watermark_start",
    "product_watermark_end",
]

for parameter_name in parameter_names:
    dbutils.widgets.text(parameter_name, "")

parameters = {
    parameter_name: dbutils.widgets.get(parameter_name).strip()
    for parameter_name in parameter_names
}

missing_parameters = sorted(
    parameter_name
    for parameter_name, value in parameters.items()
    if not value
)

if missing_parameters:
    raise RuntimeError(
        "Missing required notebook parameters: "
        + ", ".join(missing_parameters)
    )

pipeline_name = parameters["pipeline_name"]


# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

from pyspark.sql import functions as F

required_sources = {"sales", "customer", "product"}

incoming_values = [
    (
        pipeline_name,
        "sales",
        parameters["sales_watermark_start"],
        parameters["sales_watermark_end"],
    ),
    (
        pipeline_name,
        "customer",
        parameters["customer_watermark_start"],
        parameters["customer_watermark_end"],
    ),
    (
        pipeline_name,
        "product",
        parameters["product_watermark_start"],
        parameters["product_watermark_end"],
    ),
]

incoming_watermarks = (
    spark.createDataFrame(
        incoming_values,
        [
            "pipeline_name",
            "source_name",
            "watermark_start_text",
            "watermark_end_text",
        ],
    )
    .withColumn(
        "watermark_start",
        F.to_timestamp("watermark_start_text"),
    )
    .withColumn(
        "watermark_end",
        F.to_timestamp("watermark_end_text"),
    )
    .drop(
        "watermark_start_text",
        "watermark_end_text",
    )
)

# COMMAND ----------

invalid_timestamp_rows = (
    incoming_watermarks
    .filter(
        F.col("watermark_start").isNull()
        | F.col("watermark_end").isNull()
    )
    .select("source_name")
    .collect()
)

if invalid_timestamp_rows:
    invalid_sources = sorted(
        row["source_name"] for row in invalid_timestamp_rows
    )

    raise RuntimeError(
        "Invalid watermark timestamps for: "
        + ", ".join(invalid_sources)
    )

# COMMAND ----------

backward_windows = (
    incoming_watermarks
    .filter(
        F.col("watermark_end") < F.col("watermark_start")
    )
    .select(
        "source_name",
        "watermark_start",
        "watermark_end",
    )
    .collect()
)

if backward_windows:
    details = "; ".join(
        f"{row['source_name']}: "
        f"start={row['watermark_start']}, "
        f"end={row['watermark_end']}"
        for row in backward_windows
    )

    raise RuntimeError(
        f"Cannot advance backward watermark windows: {details}"
    )

# COMMAND ----------

current_watermarks = (
    spark.table("workspace.etl.dbt_source_watermarks")
    .filter(
        (F.col("pipeline_name") == pipeline_name)
        & F.col("source_name").isin(
            "sales",
            "customer",
            "product",
        )
    )
    .select(
        "pipeline_name",
        "source_name",
        "last_successful_watermark",
    )
)

control_counts = {
    row["source_name"]: row["row_count"]
    for row in (
        current_watermarks
        .groupBy("source_name")
        .count()
        .withColumnRenamed("count", "row_count")
        .collect()
    )
}

missing_sources = sorted(
    required_sources - set(control_counts)
)

duplicate_sources = sorted(
    source_name
    for source_name, row_count in control_counts.items()
    if row_count != 1
)

if missing_sources:
    raise RuntimeError(
        "Missing watermark control rows for "
        f"{pipeline_name}: {', '.join(missing_sources)}"
    )

if duplicate_sources:
    raise RuntimeError(
        "Duplicate watermark control rows for "
        f"{pipeline_name}: {', '.join(duplicate_sources)}"
    )

# COMMAND ----------

state_comparison = (
    current_watermarks.alias("current")
    .join(
        incoming_watermarks.alias("incoming"),
        on=["pipeline_name", "source_name"],
        how="inner",
    )
)

changed_starts = (
    state_comparison
    .filter(
        ~F.col("current.last_successful_watermark").eqNullSafe(
            F.col("incoming.watermark_start")
        )
    )
    .select(
        "source_name",
        "last_successful_watermark",
        "watermark_start",
    )
    .collect()
)

if changed_starts:
    details = "; ".join(
        f"{row['source_name']}: "
        f"current={row['last_successful_watermark']}, "
        f"expected_start={row['watermark_start']}"
        for row in changed_starts
    )

    raise RuntimeError(
        "Watermark control state changed after preparation. "
        f"Refusing to advance: {details}"
    )

# COMMAND ----------

incoming_watermarks.createOrReplaceTempView(
    "watermarks_to_advance"
)

# COMMAND ----------

spark.sql(
    """
    MERGE INTO workspace.etl.dbt_source_watermarks AS target
    USING watermarks_to_advance AS source
      ON target.pipeline_name = source.pipeline_name
     AND target.source_name = source.source_name

    WHEN MATCHED
     AND target.last_successful_watermark = source.watermark_start
    THEN UPDATE SET
      target.last_successful_watermark = source.watermark_end
    """
)

# COMMAND ----------

updated_watermarks = (
    spark.table("workspace.etl.dbt_source_watermarks")
    .filter(
        (F.col("pipeline_name") == pipeline_name)
        & F.col("source_name").isin(
            "sales",
            "customer",
            "product",
        )
    )
    .select(
        "pipeline_name",
        "source_name",
        "last_successful_watermark",
    )
)

verification = (
    updated_watermarks.alias("current")
    .join(
        incoming_watermarks.alias("expected"),
        on=["pipeline_name", "source_name"],
        how="inner",
    )
)

incorrect_updates = (
    verification
    .filter(
        ~F.col("current.last_successful_watermark").eqNullSafe(
            F.col("expected.watermark_end")
        )
    )
    .select(
        "source_name",
        "last_successful_watermark",
        "watermark_end",
    )
    .collect()
)

if incorrect_updates:
    details = "; ".join(
        f"{row['source_name']}: "
        f"actual={row['last_successful_watermark']}, "
        f"expected={row['watermark_end']}"
        for row in incorrect_updates
    )

    raise RuntimeError(
        f"Watermark advancement verification failed: {details}"
    )

    # COMMAND ----------

display(
    verification.select(
        "pipeline_name",
        "source_name",
        F.col("expected.watermark_start").alias(
            "previous_watermark"
        ),
        F.col("expected.watermark_end").alias(
            "advanced_watermark"
        ),
    ).orderBy("source_name")
)