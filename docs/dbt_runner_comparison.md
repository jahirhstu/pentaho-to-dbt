# Native dbt Task vs Python Notebook Runner

This document defines the comparison baseline for Step 17. The existing native
Databricks dbt task remains the control implementation until the optional Python
notebook runner proves equivalent behavior.

Fields marked **Capture in Databricks** cannot be derived from this repository.
They must be copied from the referenced workflow run before the final Step 17
adoption decision. Unknown values must not be guessed.

## 1. Native dbt task configuration

| Item | Native dbt baseline |
|---|---|
| Job | `pentaho_project_1_incremental` |
| Task | `dbt_build` |
| Task type | Native Databricks dbt task |
| Source | Git-backed repository |
| Git branch | `main` |
| Git commit | **Capture in Databricks from the baseline run** |
| Upstream dependency | `prepare_watermarks` with `All succeeded` |
| Downstream task | `advance_watermarks` with `All succeeded` |
| Compute | Databricks serverless |
| Serverless environment | Version `2` |
| Adapter package | `dbt-databricks==1.12.4` |
| dbt Core | `1.12.0` observed in the successful task logs |
| Profile target | `job` |
| Catalog | `workspace` |
| Base target schema | `dbt_dev` |
| Threads | `4` |

The effective command recorded during Step 16.7 is:

```bash
dbt build --profiles-dir . --target job --select pentaho_project_1 --vars '<six workflow-supplied watermark variables>'
```

Before running the Python comparison, copy the complete command exactly as it
appears in the Databricks `dbt_build` task into the block below. This captures
the precise quoting and dynamic-value syntax used by the UI.

```text
Exact Databricks task command: CAPTURE IN DATABRICKS
```

## 2. Watermark inputs

The native task consumes six immutable values published by the same
`prepare_watermarks` task run:

| dbt variable | Dynamic task-value reference |
|---|---|
| `sales_watermark_start` | `{{tasks.prepare_watermarks.values.sales_watermark_start}}` |
| `sales_watermark_end` | `{{tasks.prepare_watermarks.values.sales_watermark_end}}` |
| `customer_watermark_start` | `{{tasks.prepare_watermarks.values.customer_watermark_start}}` |
| `customer_watermark_end` | `{{tasks.prepare_watermarks.values.customer_watermark_end}}` |
| `product_watermark_start` | `{{tasks.prepare_watermarks.values.product_watermark_start}}` |
| `product_watermark_end` | `{{tasks.prepare_watermarks.values.product_watermark_end}}` |

The Python runner must receive these same six values. It must not query new
source maxima or independently calculate a replacement window.

Values from baseline workflow run `721943511175993`:

| Source | Start | End |
|---|---|---|
| sales | **Capture from `prepare_watermarks` task values** | **Capture from `prepare_watermarks` task values** |
| customer | **Capture from `prepare_watermarks` task values** | **Capture from `prepare_watermarks` task values** |
| product | **Capture from `prepare_watermarks` task values** | **Capture from `prepare_watermarks` task values** |

## 3. SQL warehouse

| Item | Native dbt baseline |
|---|---|
| Warehouse name | **Capture in Databricks** |
| Warehouse ID | `75e2e734a3527ecb` |
| HTTP path | `/sql/1.0/warehouses/75e2e734a3527ecb` |

The warehouse ID and HTTP path are connection configuration, not credentials.
Never copy `DBT_ACCESS_TOKEN`, a personal access token, or another secret into
this document, task parameters, command output, or notebook logs.

## 4. Successful-run measurements

Use successful workflow run `721943511175993`. Open the individual `dbt_build`
task because a parent multi-task run does not expose the individual dbt task's
output and artifact link.

| Measurement | Native dbt baseline |
|---|---|
| Parent workflow run ID | `721943511175993` |
| Individual `dbt_build` task run ID | **Capture in Databricks** |
| Git commit | **Capture in Databricks** |
| Task start timestamp | **Capture in Databricks** |
| Task end timestamp | **Capture in Databricks** |
| `dbt_build` duration | **Capture task duration, not total workflow duration** |
| Final state | Succeeded |
| Models passed | **Capture from dbt summary or `run_results.json`** |
| Tests passed | **Capture from dbt summary or `run_results.json`** |
| Warnings | **Capture in Databricks** |
| Skipped nodes | **Capture in Databricks** |
| Errors | `0` expected; confirm in task output |

## 5. dbt output and artifacts

Inspect the individual native dbt task run and record what is available:

| Output | Native task result |
|---|---|
| Inline logs | **Capture: Yes/No** |
| Logs truncated | **Capture: Yes/No** |
| Artifact archive | **Capture: Yes/No** |
| `manifest.json` | **Capture: Yes/No** |
| `run_results.json` | **Capture: Yes/No** |
| Compiled SQL | **Capture: Yes/No** |
| Other configuration artifacts | **Capture names** |
| Retention/download notes | **Capture in Databricks** |

After the Databricks CLI is configured in Step 18, the same output can be
retrieved using the individual task run ID:

```bash
databricks jobs get-run-output <dbt-task-run-id> --output json
```

Do not use the parent workflow run ID for this command.

## 6. Failure behavior

**Status: expected from the workflow configuration; not yet experimentally
verified.** Failure testing is intentionally deferred to Step 17.6 or Step
16.13.

Expected behavior:

1. A model or test failure makes dbt return a non-zero exit status.
2. Databricks marks `dbt_build` as failed.
3. `advance_watermarks` is skipped because it requires `dbt_build` to succeed.
4. `workspace.etl.dbt_source_watermarks` remains unchanged.
5. Native task logs and dbt artifacts remain available for diagnosis.

Replace the status above with **Verified**, record the failed run and task IDs,
and attach the observed evidence after the controlled failure test.

## 7. Repair procedure

**Status: designed but not yet experimentally verified.**

For a failed native `dbt_build` run:

1. Open the original failed workflow run and select **Repair run**.
2. Rerun `dbt_build` and its downstream `advance_watermarks` task.
3. Do not rerun the already-successful `prepare_watermarks` task.
4. Do not override any of the original six task values.
5. Confirm the repaired dbt task uses the original bounded window.
6. Allow `advance_watermarks` to commit the captured ends only after dbt models
   and tests succeed.

A Databricks repair uses current task settings. Before repairing, compare the
current Git/task configuration with the original run so an unrelated change is
not introduced accidentally.

## 8. Runner comparison

| Comparison item | Native dbt task | Python notebook runner |
|---|---|---|
| Command | Native `dbt build`; exact UI command pending capture | Pending Step 17.2 |
| Six watermark inputs | Fixed values from `prepare_watermarks` | Pending Step 17.2 |
| Adapter version | `dbt-databricks==1.12.4` | Pending Step 17.3 |
| dbt Core version | `1.12.0` observed | Pending Step 17.3 |
| SQL warehouse | ID `75e2e734a3527ecb` | Pending Step 17.3 |
| Serverless environment | Version `2` | Pending Step 17.3 |
| Task duration | Pending workspace capture | Pending Step 17.5 |
| Non-zero exit propagation | Native behavior; failure test pending | Pending Step 17.6 |
| Logs | Automatically captured; inventory pending | Pending Step 17.5 |
| dbt artifacts | Automatically archived; inventory pending | Pending Step 17.5 |
| Credential exposure | No token literal in repository or command | Pending security check |
| Failure blocks advancement | Expected; verification pending | Pending Step 17.6 |
| Repair preserves the window | Designed; verification pending | Pending Step 17.6 |
| Custom runner code | None | Required |

## 9. Acceptance criteria

The Python notebook runner is equivalent only if it:

- uses the same immutable Git revision for a fair comparison;
- pins `dbt-databricks==1.12.4` and confirms the effective dbt Core version;
- uses the same SQL warehouse and repository `job` profile;
- receives exactly the same six values from `prepare_watermarks`;
- runs the same `dbt build` selection, models, and tests;
- propagates a non-zero dbt exit status as a failed Databricks task;
- does not expose credentials in code, parameters, or logs;
- provides useful streaming logs plus accessible dbt artifacts;
- preserves the original bounded window during a repair;
- produces equivalent model and test results; and
- never runs concurrently with the native task against the same target tables.

The native task remains the preferred implementation unless the notebook runner
demonstrates a project requirement that outweighs its additional custom code,
artifact handling, and maintenance.

## 10. Step 17.1 completion record

The repository-backed comparison contract is complete. The fields labelled
**Capture in Databricks** form the workspace evidence checklist and must be
filled before Step 17.5 results are compared or Step 17.7 makes an adoption
decision. Failure and repair observations remain explicitly deferred rather
than being represented as verified behavior.
