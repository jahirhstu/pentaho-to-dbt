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

## Step 18.11: hourly scheduling through job as code

Version 2 has a job-specific Quartz schedule configured directly in
`resources/pentaho_project_1_incremental_v2.yml`: `0 0 * * * ?` (every hour at
minute/second zero), timezone `UTC`, and pause status `PAUSED`. No root scheduling
variables are required. Version 1 has no schedule; production remains
unconfigured. UTC avoids daylight saving changes. Quartz uses six fields here,
not the five-field Linux cron format.

### Validate and deploy paused

From the exact updated checkout in Ubuntu:

```bash
databricks bundle validate -t dev
databricks bundle validate -t dev --output json
```

Inspect `resources.jobs.pentaho_project_1_incremental_v2.schedule` and confirm:

```json
{
  "quartz_cron_expression": "0 0 * * * ?",
  "timezone_id": "UTC",
  "pause_status": "PAUSED"
}
```

Confirm version 1 remains unscheduled, the three tasks and dependencies remain
intact, and version 2 still has `max_concurrent_runs: 1`. Then deploy:

```bash
databricks bundle deploy -t dev
databricks bundle summary -t dev
```

Deployment applies all included bundle resources. Open version 2's URL from
the summary, record its ID, and confirm the schedule is hourly, UTC, and paused.
This does not start a scheduled run.

### Enable only when ready

Complete version 2's workspace acceptance checks above. Disable competing manual
and version 1 triggers and the experimental comparison task; confirm no competing
run is active. All these jobs share tables and watermark rows.

Change only the version 2 job's `schedule.pause_status` to `UNPAUSED` in
`resources/pentaho_project_1_incremental_v2.yml`, then repeat validation,
deployment, and summary commands using `-t dev`. Check
the enabled schedule and next run in the UI. An explicit resource schedule status
can override development mode's default pause behavior. Keep the intended setting
in code rather than changing only the UI. Version 1 remains available for rollback
but must not run concurrently.

After the first scheduled run, verify all three tasks succeed, dbt tests pass,
watermarks match the captured ends, and no competing workflow ran. Record the job
ID, scheduled run ID, schedule, timezone, and results in `docs/steps.txt`; only then
mark 18.11 fully Done. Notifications and production scheduling are not configured.

### Pause again

Set the same job-specific `schedule.pause_status` back to `PAUSED`, validate,
and redeploy to `dev`.
Pausing prevents future scheduled launches; it does not cancel a running job.

These job-level settings apply to every target that includes the resource unless
overridden. Before adding another operational environment, define its intended
pause policy using target overrides; an explicit job-level `UNPAUSED` setting
takes precedence over shared pause presets. Production is not ready for use.

References: [scheduled bundle examples](https://docs.databricks.com/aws/en/dev-tools/bundles/examples)
and [deployment-mode pause behavior](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes).
