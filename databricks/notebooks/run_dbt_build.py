# Databricks notebook source
"""Run the Project 1 dbt build using a fixed workflow watermark window."""

from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys


# COMMAND ----------

parameter_names = [
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


# COMMAND ----------

def parse_timestamp(parameter_name: str, value: str) -> datetime:
    """Parse one timezone-aware ISO-8601 watermark or raise a clear error."""

    normalized_value = (
        value[:-1] + "+00:00"
        if value.endswith("Z")
        else value
    )

    try:
        parsed_value = datetime.fromisoformat(normalized_value)
    except ValueError as error:
        raise ValueError(
            f"{parameter_name} is not a valid ISO-8601 timestamp"
        ) from error

    if parsed_value.tzinfo is None:
        raise ValueError(
            f"{parameter_name} must include a timezone"
        )

    return parsed_value


for source_name in ("sales", "customer", "product"):
    start_name = f"{source_name}_watermark_start"
    end_name = f"{source_name}_watermark_end"
    watermark_start = parse_timestamp(start_name, parameters[start_name])
    watermark_end = parse_timestamp(end_name, parameters[end_name])

    if watermark_end < watermark_start:
        raise ValueError(
            f"{source_name} watermark window moves backwards: "
            f"start={parameters[start_name]}, end={parameters[end_name]}"
        )


# COMMAND ----------

def find_project_root(start_directory: Path) -> Path:
    """Find the nearest parent containing the complete dbt project."""

    for candidate in (start_directory, *start_directory.parents):
        if (
            (candidate / "dbt_project.yml").is_file()
            and (candidate / "profiles.yml").is_file()
            and (candidate / "models").is_dir()
        ):
            return candidate

    raise RuntimeError(
        "Could not locate the dbt project root from "
        f"{start_directory}"
    )


project_root = find_project_root(Path.cwd().resolve())


# COMMAND ----------

def build_dbt_environment() -> dict[str, str]:
    """Create a child environment with a temporary Databricks runtime token."""

    child_environment = os.environ.copy()

    if child_environment.get("DBT_ACCESS_TOKEN"):
        return child_environment

    from databricks.sdk import WorkspaceClient

    workspace_client = WorkspaceClient(auth_type="runtime")
    authentication_headers = workspace_client.config.authenticate()
    authorization = next(
        (
            value
            for key, value in authentication_headers.items()
            if key.lower() == "authorization"
        ),
        "",
    )
    scheme, separator, access_token = authorization.partition(" ")

    if (
        separator != " "
        or scheme.lower() != "bearer"
        or not access_token
    ):
        raise RuntimeError(
            "Databricks runtime authentication did not return a bearer token"
        )

    # profiles.yml reads this variable. It is passed only to the dbt child
    # process and is never printed or written to the repository.
    child_environment["DBT_ACCESS_TOKEN"] = access_token
    return child_environment


child_environment = build_dbt_environment()


# COMMAND ----------

dbt_variables = parameters.copy()

dbt_vars_json = json.dumps(
    dbt_variables,
    separators=(",", ":"),
    sort_keys=True,
)

dbt_target_path = Path("/tmp/pentaho-to-dbt-target")
dbt_log_path = Path("/tmp/pentaho-to-dbt-logs")

dbt_target_path.mkdir(parents=True, exist_ok=True)
dbt_log_path.mkdir(parents=True, exist_ok=True)

version_check = subprocess.run(
    [sys.executable, "-m", "dbt", "--version"],
    cwd=project_root,
    env=child_environment,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)

print(version_check.stdout, end="", flush=True)

if version_check.returncode != 0:
    raise RuntimeError(
        "Unable to start dbt through the active Python interpreter; "
        f"exit code {version_check.returncode}"
    )

command = [
    sys.executable,
    "-m",
    "dbt",
    "build",
    "--project-dir",
    str(project_root),
    "--profiles-dir",
    str(project_root),
    "--target-path",
    str(dbt_target_path),
    "--log-path",
    str(dbt_log_path),
    "--target",
    "job",
    "--select",
    "pentaho_project_1",
    "--vars",
    dbt_vars_json,
]

print(f"dbt project root: {project_root}")
print(f"Python executable: {sys.executable}")
print("dbt invocation: python -m dbt")
print("dbt target: job")
print("dbt selection: pentaho_project_1")
print(f"dbt artifacts directory: {dbt_target_path}")
print(f"dbt logs directory: {dbt_log_path}")
print("Fixed watermark window:")

for parameter_name in parameter_names:
    print(f"  {parameter_name}: {parameters[parameter_name]}")


# COMMAND ----------

process = subprocess.Popen(
    command,
    cwd=project_root,
    env=child_environment,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1,
)

if process.stdout is None:
    process.kill()
    raise RuntimeError("Could not capture dbt process output")

for output_line in process.stdout:
    print(output_line, end="", flush=True)

return_code = process.wait()

if return_code != 0:
    raise RuntimeError(
        f"dbt build failed with exit code {return_code}"
    )

print("dbt build completed successfully")
print(f"Artifacts directory: {dbt_target_path}")
print(f"Logs directory: {dbt_log_path}")
