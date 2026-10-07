# Step 18.10: notebook-runner version 2

`resources/pentaho_project_1_incremental_v2.yml` adds a separate job with
`prepare_watermarks -> dbt_build -> advance_watermarks`. Version 1 is unchanged.
The existing bundle wildcard includes both resource files.

Version 2 uses `run_dbt_build.py`, the pinned serverless environment, six fixed
watermark parameters, and an explicit deployed `project_root`. The runner's
existing Git-directory discovery remains available when that parameter is empty.
Only watermark parameters enter dbt's `--vars` JSON. The `job` profile still
determines the warehouse, catalog, and schema; bundle variables do not override it.
Logs and artifacts remain in the notebook's printed `/tmp` locations and are not
durable centralized storage.

## Offline checks

From the repository root:

```bash
python3 -m unittest discover -s tests -p 'test_notebook_runner.py' -v
git diff --check
```

Tests mock dbt and authentication and do not write Databricks data. `OK` means
the local runner checks passed; it does not prove real dbt execution or workflow
repair. Checks cover explicit/legacy project discovery, invalid paths, the six
variables, invalid windows, dbt failure propagation, and unchanged retry inputs.

## Ubuntu validation and deployment

Use the exact checkout containing these changes:

```bash
databricks bundle validate -t dev
databricks bundle validate -t dev --output json
```

Inspect `resources.jobs.pentaho_project_1_incremental_v2`: three tasks, notebook
`dbt_build`, environment version `2` with `dbt-databricks==1.12.4`, resolved
project/notebook paths, six unchanged dynamic references, and both `ALL_SUCCESS`
dependencies. Check version 1 remains the native dbt job. Do not use `prod`.

After validation, deploy and record the new job ID and URL:

```bash
databricks bundle deploy -t dev
databricks bundle summary -t dev
```

Deployment updates the included bundle resources; review the changes first.
No comparison task is included in version 2. Disable any manually created
experimental comparison task before regular use.

## Workspace acceptance checks (pending)

Both versions and the manual workflow share source, output, and watermark tables.
Do not run them concurrently. Per-job `max_concurrent_runs: 1` does not provide
cross-job locking. Keep version 2 manual until verification is complete.

1. With other jobs idle, run version 2 using a controlled source-data scenario.
   Inspect notebook output, dbt tests/model results, unchanged-row `loaded_at`,
   and committed watermark ends against preparation task values.
2. In a controlled development revision, cause a dbt failure. Verify the notebook
   fails, advancement is skipped, and control watermarks remain unchanged.
3. Restore the runner and Repair run: rerun `dbt_build` and downstream tasks,
   not preparation. Verify the original six task values are reused and advancement
   commits only after success.
4. Before enabling regular runs, disable competing job triggers and the
   experimental comparison task. Keep version 1 available for rollback.

Record job/run IDs, results, and the adoption decision in `docs/steps.txt`.
