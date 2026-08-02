# Pentaho to Databricks with dbt

Learning project for converting several Pentaho ETL projects, whose transformation
logic is primarily in SQL Server stored procedures, into one dbt project targeting
Databricks.

## Architecture

```text
SQL Server sample export -> Databricks raw tables -> dbt staging -> intermediate -> marts
```

dbt transforms data already present in Databricks. For this learning project, load
small anonymized CSV or Parquet extracts manually rather than building production
ingestion.

## Project organization

Each Pentaho ETL has a domain directory and tag. Shared definitions live under
`models/shared`. Project 1 contains a deliberately small reference conversion;
replace it after analyzing the first real KJB/KTR/procedure set.

## Local setup

Prerequisites: Python 3, a virtual environment, and later a Databricks Free Edition
workspace.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
dbt --version
```

After creating Databricks Free Edition:

1. Copy `profiles.example.yml` to `~/.dbt/profiles.yml`.
2. Set `DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, and `DATABRICKS_TOKEN` in your shell.
3. Create or upload a `raw.customers` sample table matching the reference model.
4. Run:

```bash
dbt debug
dbt parse
dbt build --select tag:pentaho_project_1
```

Run all projects with `dbt build`. Run one domain with
`dbt build --select tag:pentaho_project_1`. The `+` graph operator can include
dependencies or descendants when required.

## Next migration step

Provide one small `.kjb`, all referenced `.ktr` files, the invoked stored-procedure
SQL, relevant DDL, and anonymized sample/expected data. Follow
[`docs/migration_guide.md`](docs/migration_guide.md) to inventory and convert it.

Never commit credentials or proprietary production data.
