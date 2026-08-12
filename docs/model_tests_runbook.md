# Model tests runbook

Use this runbook after the Databricks workspace, source tables, and dbt connection
have been configured. Run every command from the repository root.

## 1. Configure the dbt profile

Create the local dbt profile from the committed template:

```bash
mkdir -p ~/.dbt
cp profiles.example.yml ~/.dbt/profiles.yml
```

Set the Databricks connection values in the current terminal session:

```bash
export DATABRICKS_HOST="your-workspace-host"
export DATABRICKS_HTTP_PATH="your-warehouse-http-path"
export DATABRICKS_TOKEN="your-access-token"
```

Do not commit the access token or the populated profile.

## 2. Check configuration and connectivity

Parse the project without executing SQL:

```bash
dbt parse
```

Confirm that dbt can connect to Databricks:

```bash
dbt debug
```

List the tests selected for Project 1:

```bash
dbt list --resource-type test --select pentaho_project_1
```

## 3. Build models and execute tests

Build every Project 1 model and run its attached tests:

```bash
dbt build --select pentaho_project_1
```

Use a full refresh when the incremental models must be recreated from scratch:

```bash
dbt build --full-refresh --select pentaho_project_1
```

Run tests without rebuilding the models:

```bash
dbt test --select pentaho_project_1
```

Run only the custom business-rule tests:

```bash
dbt test --select "test_type:singular"
```

Run an individual business-rule test:

```bash
dbt test --select assert_sales_business_rules
```

The other individual business-rule test names are:

- `assert_parking_resolution_state`
- `assert_customer_summary_matches_sales`
- `assert_product_summary_matches_sales`

## 4. Store and inspect failed rows

Run the Project 1 tests and ask dbt to retain rows that violate a test:

```bash
dbt test --select pentaho_project_1 --store-failures
```

For a failed test, the terminal output identifies the Databricks relation containing
the stored failures. Inspect it in the Databricks SQL editor:

```sql
select *
from <catalog>.<test_audit_schema>.<failed_test_name>;
```

## 5. Read the results

A passing test returns no invalid rows and is displayed as `PASS`. A failure such
as `FAIL 3` means the test query returned three invalid rows. The final dbt summary
reports the total numbers of passed, warned, failed, errored, and skipped nodes.

After execution, detailed local results are available in:

- `target/run_results.json`: statuses, execution times, failure counts, and messages;
- `target/manifest.json`: the complete model and test graph;
- `target/compiled/`: compiled SQL for models and singular tests;
- `logs/dbt.log`: detailed dbt execution logs.

Generate and open dbt documentation after a successful build:

```bash
dbt docs generate
dbt docs serve
```

## Test coverage

Schema tests in `models/pentaho_project_1/pentaho_project_1.yml` cover required
values, unique keys, model relationships, boolean statuses, and the parking
rejection reason.

Custom SQL tests in `tests/` cover valid sales amounts, consistent parking
resolution timestamps, and reconciliation of the customer and product reporting
summaries with their staging data.
