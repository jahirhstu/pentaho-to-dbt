# dbt command cheat sheet

This cheat sheet contains the dbt commands used by the Pentaho-to-Databricks
migration project. Run them from the repository root unless noted otherwise.

## Local environment

Activate the project virtual environment when one is being used:

```bash
source .venv/bin/activate
```

Display the installed dbt engine and adapter versions:

```bash
dbt --version
```

Create the local profile the first time the project is configured:

```bash
mkdir -p ~/.dbt
cp profiles.example.yml ~/.dbt/profiles.yml
```

Set the Databricks connection values for the current terminal session:

```bash
export DATABRICKS_HOST="your-workspace-host"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_TOKEN="your-access-token"
```

Confirm that the variables exist without displaying their values:

```bash
test -n "$DATABRICKS_HOST" && echo "DATABRICKS_HOST is set"
test -n "$DATABRICKS_HTTP_PATH" && echo "DATABRICKS_HTTP_PATH is set"
test -n "$DATABRICKS_TOKEN" && echo "DATABRICKS_TOKEN is set"
```

Never commit the populated profile, environment variables, or access token.

## Configuration checks

Validate the profile, project files, dependencies, and Databricks connection:

```bash
dbt debug
```

Use this before the first remote build and after changing credentials, the SQL
warehouse, or `profiles.yml`.

Parse Jinja, YAML, model references, sources, and project configuration without
executing SQL in Databricks:

```bash
dbt parse
```

Use this as the fastest structural check after editing models or tests.

Compile models and tests into executable Databricks SQL without running them:

```bash
dbt compile
```

Compiled SQL is written below `target/compiled/`. Use it to understand the SQL dbt
will submit or to troubleshoot generated relation names and Jinja expressions.

## Discover project resources

List every dbt resource in the project:

```bash
dbt list
```

List Project 1 resources:

```bash
dbt list --select pentaho_project_1
```

List only Project 1 models:

```bash
dbt list --resource-type model --select pentaho_project_1
```

List only Project 1 tests:

```bash
dbt list --resource-type test --select pentaho_project_1
```

List resources carrying the Project 1 tag:

```bash
dbt list --select tag:pentaho_project_1
```

Use `dbt list` to verify a selector before using that selector with `run`, `test`,
or `build`.

## Run models

Run all models without executing tests:

```bash
dbt run
```

Run only Project 1 models:

```bash
dbt run --select pentaho_project_1
```

Run one model:

```bash
dbt run --select int_sales_customer_status
```

Run a model together with all of its upstream dependencies:

```bash
dbt run --select +rpt_customer_sales_summary
```

Run a model together with all of its downstream dependants:

```bash
dbt run --select stg_sales_source+
```

The `+` before a model includes its ancestors; the `+` after a model includes its
descendants.

## Build models and tests

Build all Project 1 models and execute the tests selected with them:

```bash
dbt build --select pentaho_project_1
```

This is the normal command for the project because it runs resources in dependency
order and tests the resulting data.

Build the complete dbt project:

```bash
dbt build
```

Recreate Project 1 incremental models from scratch and then run tests:

```bash
dbt build --full-refresh --select pentaho_project_1
```

Use `--full-refresh` for the initial controlled baseline or after an incompatible
model-structure change. It resets incremental model state, including stored parking
history, so do not use it for the normal second-snapshot recovery test.

For the recovery snapshot, use the ordinary incremental build:

```bash
dbt build --select pentaho_project_1 --vars '{
  "sales_watermark_start": "2026-07-06 09:00:00",
  "sales_watermark_end": "2026-07-07 09:10:00",
  "customer_watermark_start": "2026-07-01 08:22:00",
  "customer_watermark_end": "2026-07-07 08:00:00",
  "product_watermark_start": "2026-07-01 08:31:00",
  "product_watermark_end": "2026-07-07 08:30:00"
}'
```

Incremental Project 1 builds require all six watermark variables. Each window uses
`start < source_updated_at <= end`. In production, the Databricks workflow should
supply and persist these values only after a successful build.

## Execute tests

Run Project 1 tests without rebuilding models:

```bash
dbt test --select pentaho_project_1
```

Run only singular SQL business-rule tests:

```bash
dbt test --select "test_type:singular"
```

Run one singular test:

```bash
dbt test --select assert_sales_business_rules
```

Other singular tests in this project are:

- `assert_parking_resolution_state`
- `assert_customer_summary_matches_sales`
- `assert_product_summary_matches_sales`

Retain rows returned by failed tests in Databricks audit tables:

```bash
dbt test --select pentaho_project_1 --store-failures
```

A data test passes when its query returns zero invalid rows. `FAIL 3`, for example,
means the test returned three violating rows. See `target/run_results.json` and
`logs/dbt.log` for detailed results.

## Generate documentation

Generate the dbt catalog, manifest, and documentation files:

```bash
dbt docs generate
```

Start the local documentation website:

```bash
dbt docs serve
```

Stop the documentation server with `Ctrl+C` in its terminal.

## Dependencies and cleanup

Install packages declared in `packages.yml`:

```bash
dbt deps
```

The project currently has no external dbt packages, but this command will be needed
if packages are added later.

Delete generated directories listed under `clean-targets` in `dbt_project.yml`:

```bash
dbt clean
```

For this project, that removes `target/` and `dbt_packages/`. It does not drop
Databricks tables or schemas.

## Common working sequences

First connection and baseline build:

```bash
dbt debug
dbt parse
dbt build --full-refresh --select pentaho_project_1
```

Normal development after editing SQL or YAML:

```bash
dbt parse
dbt build --select pentaho_project_1
```

Second sample-data snapshot, preserving incremental and parking history:

```bash
dbt build --select pentaho_project_1 --vars '{
  "sales_watermark_start": "2026-07-06 09:00:00",
  "sales_watermark_end": "2026-07-07 09:10:00",
  "customer_watermark_start": "2026-07-01 08:22:00",
  "customer_watermark_end": "2026-07-07 08:00:00",
  "product_watermark_start": "2026-07-01 08:31:00",
  "product_watermark_end": "2026-07-07 08:30:00"
}'
```

Test investigation:

```bash
dbt list --resource-type test --select pentaho_project_1
dbt test --select pentaho_project_1 --store-failures
```

Documentation refresh:

```bash
dbt docs generate
dbt docs serve
```

## Generated output locations

| Location | Purpose |
|---|---|
| `target/manifest.json` | Models, tests, sources, dependencies, and resolved configuration |
| `target/run_results.json` | Execution status, timing, failures, and messages |
| `target/compiled/` | SQL after Jinja and dbt references are resolved |
| `target/catalog.json` | Warehouse metadata produced by `dbt docs generate` |
| `logs/dbt.log` | Detailed diagnostic and execution log |

The dedicated testing instructions remain available in
`docs/model_tests_runbook.md`.
