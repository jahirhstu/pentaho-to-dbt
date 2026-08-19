# Databricks notebook source
"""Capture a fixed, source-specific watermark window for one workflow run."""

from datetime import datetime, timezone
import re


# COMMAND ----------

# The job can override this widget later without changing the notebook.
dbutils.widgets.text("pipeline_name", "pentaho_project_1")
pipeline_name = dbutils.widgets.get("pipeline_name").strip()

if not re.fullmatch(r"[A-Za-z0-9_.-]+", pipeline_name):
    raise ValueError(
        "pipeline_name must contain only letters, numbers, dots, underscores, "
        "or hyphens"
    )

# Spark returns TIMESTAMP values in the configured session timezone. Keep the
# preparation task and the dbt timestamp literals consistent by using UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")


# COMMAND ----------

required_sources = {"sales", "customer", "product"}

control_rows = spark.sql(
    f"""
    select
        source_name,
        last_successful_watermark
    from workspace.etl.dbt_source_watermarks
    where pipeline_name = '{pipeline_name}'
      and source_name in ('sales', 'customer', 'product')
    """
).collect()

watermark_starts = {}

for row in control_rows:
    source_name = row["source_name"]

    if source_name in watermark_starts:
        raise RuntimeError(
            f"Duplicate watermark control row for {pipeline_name}.{source_name}"
        )

    watermark_starts[source_name] = row["last_successful_watermark"]

missing_sources = required_sources - set(watermark_starts)

if missing_sources:
    raise RuntimeError(
        "Missing watermark control rows for "
        f"{pipeline_name}: {', '.join(sorted(missing_sources))}"
    )

null_starts = sorted(
    source_name
    for source_name, value in watermark_starts.items()
    if value is None
)

if null_starts:
    raise RuntimeError(
        "Null last_successful_watermark values for "
        f"{pipeline_name}: {', '.join(null_starts)}"
    )


# COMMAND ----------

source_tables = {
    "sales": "workspace.dim.sales_source",
    "customer": "workspace.dim.dim_customer_source",
    "product": "workspace.dim.dim_product_source",
}

watermark_ends = {}

for source_name, table_name in source_tables.items():
    latest_source_timestamp = spark.sql(
        f"""
        select max(source_updated_at) as watermark_end
        from {table_name}
        """
    ).first()["watermark_end"]

    # An empty source creates an empty window instead of passing NULL to dbt.
    watermark_end = latest_source_timestamp or watermark_starts[source_name]

    if watermark_end < watermark_starts[source_name]:
        raise RuntimeError(
            f"Source watermark moved backwards for {source_name}: "
            f"start={watermark_starts[source_name]!s}, end={watermark_end!s}"
        )

    watermark_ends[source_name] = watermark_end


# COMMAND ----------

def format_utc_timestamp(value: datetime) -> str:
    """Return an ISO-8601 timestamp that Databricks SQL can cast unambiguously."""

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)

    return value.isoformat(timespec="microseconds")


published_values = []

for source_name in sorted(required_sources):
    start_value = format_utc_timestamp(watermark_starts[source_name])
    end_value = format_utc_timestamp(watermark_ends[source_name])

    dbutils.jobs.taskValues.set(
        key=f"{source_name}_watermark_start",
        value=start_value,
    )
    dbutils.jobs.taskValues.set(
        key=f"{source_name}_watermark_end",
        value=end_value,
    )

    published_values.append(
        {
            "pipeline_name": pipeline_name,
            "source_name": source_name,
            "watermark_start": start_value,
            "watermark_end": end_value,
        }
    )

display(spark.createDataFrame(published_values).orderBy("source_name"))
