# Databricks notebook source
pip install office365-rest-python-client --trusted-host pypi.org --trusted-host files.pythonhosted.org openpyxl

# COMMAND ----------

import io
import pandas as pd
from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

try:
    tenant_id = ""
    client_id = ""
    client_secret = ""

    site_url = "https://share.philips.com/sites/RI-CPMExcellence" 
    file_relative_url = "/sites/RI-CPMExcellence/Shared Documents/General/Dashboard/input/final_project_account_mapping.xlsx"

    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(site_url).with_credentials(credentials)

    # Download the file's raw bytes
    response = File.open_binary(ctx, file_relative_url)
    file_content = response.content

    # Load into a DataFrame
    df = pd.read_excel(io.BytesIO(file_content),sheet_name="Sheet1")


    # # If you want record-by-record access like SharePoint list items:
    # records = df.to_dict(orient="records")
    # for record in records:
    #     print(record)

except Exception as e:
    print(f"Connection establishment with Sharepoint failed with error: {e}")

# COMMAND ----------

PROJECT_IDS = df["Reqtest Project ID"].dropna().astype("int64").unique().tolist()

# COMMAND ----------

"""
ReQtest API Fetcher (Optimized)
--------------------------------
Fixes:
  - Pagination handling to prevent duplicate rows
  - Single connection test reused across projects
  - Configurable rate-limiting with min interval between calls
  - Removed redundant sleep() calls; replaced with a single throttle helper
  - Testcase detail fetching skipped if testcases list is empty

Stores results in four pandas DataFrames and exports to Excel with four sheets:
  - "Testruns"
  - "Testruns Content"
  - "Testcases"
  - "Testcase Details"

Compatible with: Local Python & Databricks notebooks

Configuration:
    Edit PAT and PROJECT_IDS below, or set environment variables:
        REQTEST_PAT=your_token
        REQTEST_PROJECT_IDS=82128,82129,82130

Output:
    Local      → reqtest_output.xlsx (current working directory)
    Databricks → /dbfs/tmp/reqtest_output.xlsx
"""

import os
import time
import requests
import urllib3
import pandas as pd

# ── DETECT ENVIRONMENT ────────────────────────────────────────────────────────

def is_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

IS_DATABRICKS = is_databricks()
print(f"[ENV] Running in {'Databricks' if IS_DATABRICKS else 'Local Python'}")

# ── SSL WORKAROUND FOR DATABRICKS ─────────────────────────────────────────────

if IS_DATABRICKS:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    print("[SSL] WARNING: SSL verification disabled due to empty CA store on this cluster.")
    print("      Ask your admin to fix the cluster CA bundle for a permanent solution.\n")

SSL_VERIFY = not IS_DATABRICKS

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

PAT = dbutils.secrets.get(scope="reqtest1", key="PAT_Token")        
# Set via env or paste directly
#PROJECT_IDS = os.environ.get("REQTEST_PROJECT_IDS", "").split(",") if os.environ.get("REQTEST_PROJECT_IDS") else []
# PROJECT_IDS = ["79671","81084","79540","80398","78671","80151","82001"]  # ← uncomment & edit

API_BASE        = "https://secure.reqtest.com/api/v3"
REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS  = 3
RETRY_BACKOFF   = 2        # seconds between retries
MIN_CALL_GAP    = 1.0      # minimum seconds between any two API calls (rate limiting)

# ── RATE LIMITER ──────────────────────────────────────────────────────────────

_last_call_time = 0.0

def throttle():
    """Ensure at least MIN_CALL_GAP seconds between API calls."""
    global _last_call_time
    elapsed = time.time() - _last_call_time
    if elapsed < MIN_CALL_GAP:
        time.sleep(MIN_CALL_GAP - elapsed)
    _last_call_time = time.time()

# ── ABORT HELPER ──────────────────────────────────────────────────────────────

def abort(msg):
    raise RuntimeError(msg)

# ── API HELPERS ───────────────────────────────────────────────────────────────

def get_headers():
    return {"Accept": "application/json", "ReQtest-PAT": PAT}


def api_get(url, params=None):
    """
    GET a single page. Raises RuntimeError after RETRY_ATTEMPTS failures.
    Uses throttle() before every real HTTP call.
    """
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            throttle()
            resp = requests.get(
                url,
                headers=get_headers(),
                params=params,
                timeout=REQUEST_TIMEOUT,
                verify=SSL_VERIFY,
            )
            resp.raise_for_status()
            data = resp.json()
            if not data.get("isSuccess"):
                raise ValueError(f"API error: {data.get('message')}")
            return data["result"]
        except (requests.RequestException, ValueError) as e:
            print(f"  [WARN] Attempt {attempt}/{RETRY_ATTEMPTS} failed — {e}")
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF)
            else:
                raise RuntimeError(f"All {RETRY_ATTEMPTS} attempts failed for {url}") from e


def api_get_paged(url, list_key, page_size=100):
    """
    Fetch ALL pages for a paginated endpoint.

    Tries common ReQtest pagination patterns:
      1. ?page=N&pageSize=M  (1-based page index)
      2. ?offset=N&limit=M   (offset-based)
    Stops when a page returns fewer items than page_size.

    Args:
        url       : base endpoint URL (no query string)
        list_key  : key inside result that holds the list, e.g. "testruns"
        page_size : items per page (default 100; lower if the API caps earlier)

    Returns:
        list of all items across all pages (deduplicated by 'id' if present)
    """
    all_items = []
    seen_ids  = set()
    page      = 1

    while True:
        params = {"page": page, "pageSize": page_size}
        result = api_get(url, params=params)

        # result may be a list or a dict containing the list
        if isinstance(result, list):
            items = result
        else:
            items = (
                result.get(list_key)
                or result.get(list_key.replace("_", ""))   # camelCase fallback
                or []
            )

        if not items:
            break   # no more data

        # Deduplication guard — stops infinite loops if API ignores pagination params
        new_items = []
        for item in items:
            item_id = item.get("id")
            if item_id is None or item_id not in seen_ids:
                new_items.append(item)
                if item_id is not None:
                    seen_ids.add(item_id)

        all_items.extend(new_items)

        if len(new_items) == 0:
            # All items on this page were duplicates → definitely finished
            print(f"    [PAGE] Stopping at page {page} — all items already seen (dedup guard).")
            break

        if len(items) < page_size:
            break   # last page (partial)

        page += 1

    return all_items


def test_connection():
    """
    Validate PAT and basic connectivity by calling the /projects endpoint
    (lists all projects) — no specific project ID needed.
    A 200 OR 403 both confirm the API is reachable and the PAT is recognised;
    only a network error or a 401 'invalid token' response means the PAT is bad.
    Per-project existence is checked separately inside the main loop.
    Returns True/False.
    """
    print("[CHECK] Testing API connectivity via /projects list endpoint...")
    try:
        throttle()
        resp = requests.get(
            f"{API_BASE}/projects",
            headers=get_headers(),
            timeout=REQUEST_TIMEOUT,
            verify=SSL_VERIFY,
        )
        # 200 = success, 403 = PAT valid but org-level list restricted (still reachable)
        if resp.status_code in (200, 403):
            print("[CHECK] ✓ API reachable and PAT is valid.\n")
            return True
        # 401 = PAT completely rejected — abort makes sense here
        if resp.status_code == 401:
            print(f"[CHECK] ✗ HTTP 401 — PAT is invalid or expired. Check your token.\n")
            return False
        # Any other unexpected status
        print(f"[CHECK] ✗ HTTP {resp.status_code} — unexpected response.")
        print(f"        Response: {resp.text[:300]}\n")
        return False
    except requests.ConnectionError as e:
        print(f"[CHECK] ✗ Connection error — {e}")
        if IS_DATABRICKS:
            print("        Ensure outbound internet is enabled on your cluster.\n")
        return False
    except requests.Timeout:
        print("[CHECK] ✗ Connection timed out.\n")
        return False
    except Exception as e:
        print(f"[CHECK] ✗ Unexpected error — {e}\n")
        return False


def check_project_exists(project_id):
    """
    Check whether a specific project ID exists and is accessible.
    Returns (exists: bool, reason: str).
    Does NOT raise — safe to call inside the main loop.

    Note: ReQtest returns HTTP 401 with error code 4000016 when a project
    does not exist or the PAT lacks access to it — this is treated as
    'missing/no access', NOT as an invalid PAT (which is caught in test_connection).
    """
    try:
        throttle()
        resp = requests.get(
            f"{API_BASE}/projects/{project_id}",
            headers=get_headers(),
            timeout=REQUEST_TIMEOUT,
            verify=SSL_VERIFY,
        )
        if resp.status_code == 200:
            data = resp.json()
            if not data.get("isSuccess", True):
                return False, f"API returned isSuccess=false: {data.get('message', 'unknown')}"
            return True, "OK"
        if resp.status_code == 404:
            return False, "Project not found (404)"
        if resp.status_code == 401:
            # ReQtest uses 401 for "project not found or no access" (code 4000016)
            # This is a per-project permission issue, NOT an invalid PAT
            try:
                msg = resp.json().get("exception", {}).get("message", resp.text[:200])
            except Exception:
                msg = resp.text[:200]
            return False, f"No access or project does not exist (401): {msg}"
        if resp.status_code == 403:
            return False, f"Access denied (403) — PAT lacks permission for this project"
        return False, f"Unexpected HTTP {resp.status_code}"
    except requests.Timeout:
        return False, "Request timed out"
    except Exception as e:
        return False, str(e)

# ── FETCH FUNCTIONS ───────────────────────────────────────────────────────────

def fetch_testruns(project_id):
    return api_get_paged(f"{API_BASE}/projects/{project_id}/testruns", list_key="testruns")


def fetch_contents(project_id, testrun_id):
    return api_get_paged(
        f"{API_BASE}/projects/{project_id}/testruns/{testrun_id}/contents",
        list_key="contents",
    )


def fetch_testcases(project_id):
    return api_get_paged(f"{API_BASE}/projects/{project_id}/testcases", list_key="testcases")


def fetch_testcase_detail(project_id, testcase_id):
    """Single-item endpoint — no pagination needed."""
    return api_get(f"{API_BASE}/projects/{project_id}/testcases/{testcase_id}")

# ── ROW BUILDERS ──────────────────────────────────────────────────────────────

def build_testrun_row(project_id, tr):
    return {
        "project_id":         project_id,
        "id":                 tr.get("id"),
        "custom_id":          tr.get("customId"),
        "type":               tr.get("type"),
        "created_by":         tr.get("createdBy"),
        "created_by_user_id": tr.get("createdByUserId"),
        "archived":           tr.get("archived"),
        "created_date":       tr.get("createdDate"),
        "changed_date":       tr.get("changedDate"),
    }


def build_content_row(project_id, testrun_id, c):
    return {
        "project_id":          project_id,
        "testrun_id":          testrun_id,
        "id":                  c.get("id"),
        "name":                c.get("name"),
        "type":                c.get("type"),
        "test_suite_id":       c.get("testSuiteId"),
        "test_suite_name":     c.get("testSuiteName"),
        "executed_by":         c.get("executedBy"),
        "executed_by_user_id": c.get("executedByUserId"),
        "execution_date":      c.get("executionDate"),
        "result":              c.get("result"),
        "result_text":         c.get("resultText"),
        "pre_conditions":      c.get("preConditions"),
        "test_case":           c.get("testCase"),
        "links":               c.get("links"),
    }


def build_testcase_row(project_id, tc):
    return {
        "project_id":         project_id,
        "id":                 tc.get("id"),
        "custom_id":          tc.get("customId"),
        "name":               tc.get("name"),
        "type":               tc.get("type"),
        "created_by":         tc.get("createdBy"),
        "created_by_user_id": tc.get("createdByUserId"),
        "archived":           tc.get("archived"),
        "created_date":       tc.get("createdDate"),
        "changed_date":       tc.get("changedDate"),
        "test_suite_id":      tc.get("testSuiteId"),
        "test_suite_name":    tc.get("testSuiteName"),
    }


def build_testcase_detail_row(project_id, testcase_id, detail):
    return {
        "project_id":         project_id,
        "testcase_id":        testcase_id,
        "id":                 detail.get("id"),
        "custom_id":          detail.get("customId"),
        "name":               detail.get("name"),
        "type":               detail.get("type"),
        "created_by":         detail.get("createdBy"),
        "created_by_user_id": detail.get("createdByUserId"),
        "archived":           detail.get("archived"),
        "created_date":       detail.get("createdDate"),
        "changed_date":       detail.get("changedDate"),
        "pre_conditions":     detail.get("preConditions"),
        "description":        detail.get("description"),
        "expected_result":    detail.get("expectedResult"),
        "test_steps":         detail.get("testSteps"),
        "test_suite_id":      detail.get("testSuiteId"),
        "test_suite_name":    detail.get("testSuiteName"),
        "priority":           detail.get("priority"),
        "status":             detail.get("status"),
        "links":              detail.get("links"),
        "tags":               detail.get("tags"),
    }

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    if not PAT:
        abort("[ERROR] PAT is not set. Add it to the CONFIGURATION section or set REQTEST_PAT env var.")
    if not PROJECT_IDS or PROJECT_IDS == [""]:
        abort("[ERROR] No project IDs configured.")

    # ── Single connectivity + PAT check before entering the loop ─────────────
    # if not test_connection():
    #     abort("[ERROR] Cannot reach ReQtest API or PAT is invalid. Aborting.")

    all_testruns  = []
    all_contents  = []
    all_testcases = []
    all_tc_detail = []

    # ── Project-level tracking ────────────────────────────────────────────────
    projects_found   = []   # IDs that exist and were fetched successfully
    projects_missing = []   # IDs that returned 404 / not found
    projects_errored = []   # IDs that failed for other reasons (auth, timeout, etc.)

    for project_id in PROJECT_IDS:
        print(f"\n[PROJECT {project_id}] ────────────────────────────────")

        # ── Per-project existence check (no abort) ────────────────────────────
        exists, reason = check_project_exists(project_id)
        if not exists:
            if "404" in reason or "not found" in reason.lower():
                print(f"  [SKIP] Project {project_id} does not exist in the API — {reason}")
                projects_missing.append({"project_id": project_id, "reason": reason})
            else:
                print(f"  [SKIP] Project {project_id} skipped due to error — {reason}")
                projects_errored.append({"project_id": project_id, "reason": reason})
            continue   # move on to the next project; do NOT abort

        projects_found.append(project_id)

        # ── Testruns ──────────────────────────────────────────────────────────
        try:
            testruns = fetch_testruns(project_id)
        except Exception as e:
            print(f"  [ERROR] Could not fetch testruns: {e}")
            testruns = []

        print(f"  Found {len(testruns)} testrun(s).")

        for tr in testruns:
            all_testruns.append(build_testrun_row(project_id, tr))

            testrun_id = tr.get("id")
            print(f"  → Fetching contents for testrun {testrun_id}...")
            try:
                contents = fetch_contents(project_id, testrun_id)
            except Exception as e:
                print(f"    [ERROR] Could not fetch contents: {e}")
                contents = []

            print(f"    Found {len(contents)} unique item(s).")
            for c in contents:
                all_contents.append(build_content_row(project_id, testrun_id, c))

        # ── Testcases list ────────────────────────────────────────────────────
        print(f"\n  Fetching testcases for project {project_id}...")
        try:
            testcases = fetch_testcases(project_id)
        except Exception as e:
            print(f"  [ERROR] Could not fetch testcases: {e}")
            testcases = []

        print(f"  Found {len(testcases)} testcase(s).")

        for tc in testcases:
            all_testcases.append(build_testcase_row(project_id, tc))

            testcase_id = tc.get("id")
            print(f"  → Fetching detail for testcase {testcase_id}...")
            try:
                detail = fetch_testcase_detail(project_id, testcase_id)
                all_tc_detail.append(build_testcase_detail_row(project_id, testcase_id, detail))
            except Exception as e:
                print(f"    [ERROR] Could not fetch detail for testcase {testcase_id}: {e}")
                all_tc_detail.append({
                    "project_id":  project_id,
                    "testcase_id": testcase_id,
                    "fetch_error": str(e),
                })

    # ── Build DataFrames ──────────────────────────────────────────────────────
    df_testruns        = pd.DataFrame(all_testruns)
    df_contents        = pd.DataFrame(all_contents)
    df_testcases       = pd.DataFrame(all_testcases)
    df_testcase_detail = pd.DataFrame(all_tc_detail)

    # ── Project availability summary ──────────────────────────────────────────
    df_projects_found   = pd.DataFrame({"project_id": projects_found, "status": "found"})
    df_projects_missing = pd.DataFrame(projects_missing).assign(status="missing") if projects_missing else pd.DataFrame(columns=["project_id", "reason", "status"])
    df_projects_errored = pd.DataFrame(projects_errored).assign(status="error")   if projects_errored else pd.DataFrame(columns=["project_id", "reason", "status"])

    print("\n" + "═" * 55)
    print("[SUMMARY] Project Availability")
    print("═" * 55)
    print(f"  ✓ Found   ({len(projects_found)})  : {projects_found}")
    if projects_missing:
        print(f"  ✗ Missing ({len(projects_missing)}) :")
        for m in projects_missing:
            print(f"      {m['project_id']} — {m['reason']}")
    if projects_errored:
        print(f"  ⚠ Errored ({len(projects_errored)}) :")
        for e in projects_errored:
            print(f"      {e['project_id']} — {e['reason']}")
    print("═" * 55)

    print(f"\n[DONE] Testruns rows        : {len(df_testruns)}")
    print(f"[DONE] Contents rows        : {len(df_contents)}")
    print(f"[DONE] Testcases rows       : {len(df_testcases)}")
    print(f"[DONE] Testcase Detail rows : {len(df_testcase_detail)}")

    # ── Previews ──────────────────────────────────────────────────────────────
    for label, df in [
        ("Testruns",         df_testruns),
        ("Testruns Content", df_contents),
        ("Testcases",        df_testcases),
        ("Testcase Details", df_testcase_detail),
    ]:
        if not df.empty:
            print(f"\n── {label} (preview) ──────────────────────────────")
            print(df.head(5).to_string(index=False))

    

    # ── Final merged DataFrame ────────────────────────────────────────────────
    if not df_testruns.empty and not df_contents.empty:
        df_testruns_slim = df_testruns[["project_id", "custom_id", "id"]]

        df_contents_slim = df_contents[[
            "testrun_id", "type", "id", "name", "result_text", "test_suite_id", "test_suite_name"
        ]].rename(columns={
            "testrun_id":      "Test Run ID",
            "test_suite_name": "Test Suite Name",
            "type":            "Type",
            "id":              "Test Run Content ID",
            "test_suite_id":   "Test Suite ID",
            "name":            "Test Run Name",
            "result_text":     "Execution Result",
        })

        df_reqtest = (
            df_testruns_slim
            .merge(df_contents_slim, left_on="id", right_on="Test Run ID", how="inner")
            .rename(columns={"project_id": "Project ID", "custom_id": "Custom ID"})
            [["Project ID", "Custom ID", "Test Run ID", "Test Suite Name",
              "Type", "Test Run Content ID", "Test Suite ID", "Test Run Name", "Execution Result"]]
        )

        print(f"\n[MERGED] df_reqtest rows: {len(df_reqtest)}")
        print(df_reqtest.head(5).to_string(index=False))
    else:
        df_reqtest = pd.DataFrame()
        print("\n[MERGED] Skipped — testruns or contents DataFrame is empty.")

    return (
        df_testruns, df_contents, df_testcases, df_testcase_detail, df_reqtest,
        projects_found, projects_missing, projects_errored,
    )

# ── ENTRY POINT ───────────────────────────────────────────────────────────────

(
    df_testruns, df_contents, df_testcases, df_testcase_detail, df_reqtest,
    projects_found, projects_missing, projects_errored,
) = main()

# COMMAND ----------

df_reqtest = df_reqtest.rename(columns={
    "Project ID":          "project_id",
    "Custom ID":           "custom_id",
    "Test Run ID":         "test_run_id",
    "Test Suite Name":      "test_suite_name",
    "Type":                "type",
    "Test Run Content ID": "test_run_content_id",
    "Test Suite ID":       "test_suite_id",
    "Test Run Name":               "test_run_name",
    "Execution Result":    "execution_result",
})

# COMMAND ----------

# Convert Pandas df to Spark df, then write
spark_df = spark.createDataFrame(df_reqtest)

spark_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://wb-saasfactory@az21q1datalakewe.dfs.core.windows.net/Gold/cpm/g_fct_cpm_reqtest")