# Step 18.1: Install and authenticate the Databricks CLI

This guide covers Step 18.1 in [steps.txt](steps.txt): install the Databricks CLI on your development machine, create a local `pentaho-dev` authentication profile, and verify workspace access with a read-only command. Run the commands on the machine where you will work on the bundle. Installing dbt in a Python virtual environment is separate from installing this CLI.

Workspace URL for this project: `https://dbc-6a1c9cc6-e36f.cloud.databricks.com`.

## Windows (PowerShell)

Install the current CLI with Windows Package Manager, then open a new PowerShell window:

```powershell
winget install Databricks.DatabricksCLI
databricks version
```

If `winget` is unavailable, use another Windows installation method from the [official Databricks CLI installation guide](https://docs.databricks.com/aws/en/dev-tools/cli/install). The version must be `0.205.0` or newer.

If Windows has a browser, sign in with OAuth:

```powershell
databricks auth login --host https://dbc-6a1c9cc6-e36f.cloud.databricks.com --profile pentaho-dev
```

Complete the browser prompt. If browser login is unavailable, use the personal access token method below.

## Ubuntu (including a headless VirtualBox VM)

Install `curl` and `unzip`, then install the CLI:

```bash
sudo apt update
sudo apt install -y curl unzip
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks version
```

If the installer says `/usr/local/bin` is not writable, rerun **only the installation command** with elevated permissions:

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
databricks version
```

The version must be `0.205.0` or newer. The CLI is installed system-wide, independently of a Python `.venv`.

For Ubuntu with a working browser, use the same OAuth login command as above. A headless VirtualBox VM has no browser, and opening its login URL on the Mac alone may not complete the callback to the VM. For that setup, use the token method below.

## Browser-free authentication with a personal access token

On a computer with a browser, open this project's Databricks workspace. Click your username in the top bar, then **Settings → Developer → Access tokens → Manage → Generate new token**. Give the token a recognizable name and an expiry, select scopes that allow the workspace operations you need, generate it, and copy its value immediately. Databricks will not show the value again. If token creation is unavailable, ask your workspace administrator whether personal access tokens are enabled; an alternative is OAuth with a browser and a connection that forwards the login callback to the VM. See [Databricks' token instructions](https://docs.databricks.com/aws/en/dev-tools/auth/pat).

On the development machine, run the following command in PowerShell or Ubuntu Terminal:

```text
databricks configure --host https://dbc-6a1c9cc6-e36f.cloud.databricks.com --profile pentaho-dev
```

Paste the token only when prompted for `Databricks Token`. Do not put it in the command line, project files, screenshots, or chat. This command writes the profile to `~/.databrickscfg` on Ubuntu or `%USERPROFILE%\.databrickscfg` on Windows. Keep that file private and outside the Git repository. See [CLI configuration profiles](https://docs.databricks.com/aws/en/dev-tools/cli/reference/configure-commands).

## Verify Step 18.1

Run these read-only commands on the machine you configured:

```text
databricks version
databricks auth profiles -o text
databricks current-user me --profile pentaho-dev
```

Confirm that the CLI version is at least `0.205.0`, the `pentaho-dev` profile appears, and `current-user me` returns your Databricks identity. These checks do not change workspace data.

If `current-user me` fails even though the profile exists, check whether `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, or `DATABRICKS_CONFIG_PROFILE` is set in that terminal: environment variables can override a named profile. Remove conflicting variables from that terminal and retry. If the token has expired or was revoked, create a new one and rerun `databricks configure`.

Once these checks pass, Step 18.1 is complete. The `pentaho-dev` profile can be selected explicitly when working on Step 18.2; the profile and its credentials stay local.
