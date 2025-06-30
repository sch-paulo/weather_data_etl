import functions_framework
import subprocess
import os
import shutil
import logging

# --- Configuration ---
GIT_REPO_URL = "https://github.com/sch-paulo/weather_data_etl_dbt.git"
DBT_PROJECT_DIR = "weather_data_etl_dbt" # The name of the repo folder

# Setup logging
logging.basicConfig(level=logging.INFO)

def run_dbt_command(command, project_dir):
    """Runs a dbt command in a subprocess."""
    logging.info(f"Running command: {command}")
    try:
        # The 'capture_output=True' and 'text=True' arguments capture stdout/stderr.
        # The 'check=True' argument raises an exception for non-zero exit codes.
        process = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            cwd=project_dir, # Run command inside the dbt project directory
            env=os.environ.copy() # Pass environment variables to the subprocess
        )
        logging.info("dbt command stdout:\n" + process.stdout)
        if process.stderr:
            logging.warning("dbt command stderr:\n" + process.stderr)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"dbt command failed with exit code {e.returncode}")
        logging.error("dbt command stdout:\n" + e.stdout)
        logging.error("dbt command stderr:\n" + e.stderr)
        return False

@functions_framework.http
def run_dbt_models(request):
    """
    An HTTP-triggered Cloud Function that clones a Git repo,
    and runs dbt commands.
    """
    # The /tmp directory is a writable in-memory filesystem in Cloud Functions.
    local_repo_path = f"/tmp/{DBT_PROJECT_DIR}"

    # --- 1. Clean up previous run and Clone Git Repository ---
    if os.path.exists(local_repo_path):
        logging.info(f"Removing existing directory: {local_repo_path}")
        shutil.rmtree(local_repo_path)

    logging.info(f"Cloning repo {GIT_REPO_URL} into {local_repo_path}")
    # We don't need the full git history, so a shallow clone is faster.
    clone_command = ["git", "clone", "--depth", "1", GIT_REPO_URL, local_repo_path]
    subprocess.run(clone_command, check=True)

    dbt_project_path = local_repo_path

    # --- 2. Run dbt Commands ---
    # The Cloud Function's service account provides authentication to BigQuery,
    # so no profiles.yml is needed. dbt-bigquery will use the default credentials.
    
    # Run dbt deps (if you add packages later)
    if not run_dbt_command(["dbt", "deps"], project_dir=dbt_project_path):
        return "dbt deps failed", 500

    # Run dbt build
    if not run_dbt_command(["dbt", "run"], project_dir=dbt_project_path):
        return "dbt run failed", 500

    logging.info("dbt commands executed successfully.")
    return "dbt models executed successfully", 200
