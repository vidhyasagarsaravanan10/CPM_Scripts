-- Databricks notebook source
MERGE INTO qa_wb.saasfactory.g_fct_cpm_incident_hist_new t
USING prod_l2.services.tbl_ocs_ei_incident s
ON t.incident_number = s.incident_number
AND t.is_current = true

WHEN MATCHED AND (
    COALESCE(t.close_notes,'') <> COALESCE(s.close_notes,'')
 OR COALESCE(t.priority,'') <> COALESCE(s.priority,'')
 OR COALESCE(t.contact_type,'') <> COALESCE(s.contact_type,'')
 OR COALESCE(t.opened_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
    <> COALESCE(s.opened_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
 OR COALESCE(t.resolved_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
    <> COALESCE(s.resolved_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
 OR COALESCE(t.company,'') <> COALESCE(s.company,'')
 OR COALESCE(t.is_active,false) <> COALESCE(s.is_active,false)
)
THEN UPDATE SET
    t.is_current = false;

-- COMMAND ----------

INSERT INTO qa_wb.saasfactory.g_fct_cpm_incident_hist_new
(
    incident_number,
    close_notes,
    priority,
    contact_type,
    opened_on_timestamp,
    resolved_on_timestamp,
    company,
    is_active,
    is_current,
    load_date
)
SELECT
    s.incident_number,
    s.close_notes,
    s.priority,
    s.contact_type,
    s.opened_on_timestamp,
    s.resolved_on_timestamp,
    s.company,
    s.is_active,
    true,
    current_date()
FROM prod_l2.services.tbl_ocs_ei_incident s
LEFT JOIN qa_wb.saasfactory.g_fct_cpm_incident_hist_new t
    ON s.incident_number = t.incident_number
    AND t.is_current = true
WHERE
    s.incident_number IS NOT NULL          -- 🔑 guard against blank/null source keys
    AND (
        t.incident_number IS NULL
        OR
        (
            COALESCE(t.close_notes,'') <> COALESCE(s.close_notes,'')
         OR COALESCE(t.priority,'') <> COALESCE(s.priority,'')
         OR COALESCE(t.contact_type,'') <> COALESCE(s.contact_type,'')
         OR COALESCE(t.opened_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
            <> COALESCE(s.opened_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
         OR COALESCE(t.resolved_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
            <> COALESCE(s.resolved_on_timestamp, TIMESTAMP'1900-01-01 00:00:00')
         OR COALESCE(t.company,'') <> COALESCE(s.company,'')
         OR COALESCE(t.is_active,false) <> COALESCE(s.is_active,false)
        )
    );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC REQTEST PROCESS

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC
-- MAGIC rows = spark.sql("""
-- MAGIC     select distinct reqtest_project_id 
-- MAGIC     from qa_wb.saasfactory.g_dim_cpm_project_acct_mapping_new
-- MAGIC """).collect()
-- MAGIC
-- MAGIC # Convert to a plain Python list
-- MAGIC PROJECT_IDS = [row["reqtest_project_id"] for row in rows]

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC """
-- MAGIC ReQtest API Fetcher (Optimized)
-- MAGIC --------------------------------
-- MAGIC Fixes:
-- MAGIC   - Pagination handling to prevent duplicate rows
-- MAGIC   - Single connection test reused across projects
-- MAGIC   - Configurable rate-limiting with min interval between calls
-- MAGIC   - Removed redundant sleep() calls; replaced with a single throttle helper
-- MAGIC   - Testcase detail fetching skipped if testcases list is empty
-- MAGIC
-- MAGIC Stores results in four pandas DataFrames and exports to Excel with four sheets:
-- MAGIC   - "Testruns"
-- MAGIC   - "Testruns Content"
-- MAGIC   - "Testcases"
-- MAGIC   - "Testcase Details"
-- MAGIC
-- MAGIC Compatible with: Local Python & Databricks notebooks
-- MAGIC
-- MAGIC Configuration:
-- MAGIC     Edit PAT and PROJECT_IDS below, or set environment variables:
-- MAGIC         REQTEST_PAT=your_token
-- MAGIC         REQTEST_PROJECT_IDS=82128,82129,82130
-- MAGIC
-- MAGIC Output:
-- MAGIC     Local      → reqtest_output.xlsx (current working directory)
-- MAGIC     Databricks → /dbfs/tmp/reqtest_output.xlsx
-- MAGIC """
-- MAGIC
-- MAGIC import os
-- MAGIC import time
-- MAGIC import requests
-- MAGIC import urllib3
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # ── DETECT ENVIRONMENT ────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC def is_databricks():
-- MAGIC     return "DATABRICKS_RUNTIME_VERSION" in os.environ
-- MAGIC
-- MAGIC IS_DATABRICKS = is_databricks()
-- MAGIC print(f"[ENV] Running in {'Databricks' if IS_DATABRICKS else 'Local Python'}")
-- MAGIC
-- MAGIC # ── SSL WORKAROUND FOR DATABRICKS ─────────────────────────────────────────────
-- MAGIC
-- MAGIC if IS_DATABRICKS:
-- MAGIC     urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
-- MAGIC     print("[SSL] WARNING: SSL verification disabled due to empty CA store on this cluster.")
-- MAGIC     print("      Ask your admin to fix the cluster CA bundle for a permanent solution.\n")
-- MAGIC
-- MAGIC SSL_VERIFY = not IS_DATABRICKS
-- MAGIC
-- MAGIC # ── CONFIGURATION ─────────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC PAT = dbutils.secrets.get(scope="reqtest1", key="PAT_Token")        
-- MAGIC # Set via env or paste directly
-- MAGIC #PROJECT_IDS = os.environ.get("REQTEST_PROJECT_IDS", "").split(",") if os.environ.get("REQTEST_PROJECT_IDS") else []
-- MAGIC # PROJECT_IDS = ["79671","81084","79540","80398","78671","80151","82001"]  # ← uncomment & edit
-- MAGIC
-- MAGIC API_BASE        = "https://secure.reqtest.com/api/v3"
-- MAGIC REQUEST_TIMEOUT = 30
-- MAGIC RETRY_ATTEMPTS  = 3
-- MAGIC RETRY_BACKOFF   = 2        # seconds between retries
-- MAGIC MIN_CALL_GAP    = 1.0      # minimum seconds between any two API calls (rate limiting)
-- MAGIC
-- MAGIC # ── RATE LIMITER ──────────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC _last_call_time = 0.0
-- MAGIC
-- MAGIC def throttle():
-- MAGIC     """Ensure at least MIN_CALL_GAP seconds between API calls."""
-- MAGIC     global _last_call_time
-- MAGIC     elapsed = time.time() - _last_call_time
-- MAGIC     if elapsed < MIN_CALL_GAP:
-- MAGIC         time.sleep(MIN_CALL_GAP - elapsed)
-- MAGIC     _last_call_time = time.time()
-- MAGIC
-- MAGIC # ── ABORT HELPER ──────────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC def abort(msg):
-- MAGIC     raise RuntimeError(msg)
-- MAGIC
-- MAGIC # ── API HELPERS ───────────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC def get_headers():
-- MAGIC     return {"Accept": "application/json", "ReQtest-PAT": PAT}
-- MAGIC
-- MAGIC
-- MAGIC def api_get(url, params=None):
-- MAGIC     """
-- MAGIC     GET a single page. Raises RuntimeError after RETRY_ATTEMPTS failures.
-- MAGIC     Uses throttle() before every real HTTP call.
-- MAGIC     """
-- MAGIC     for attempt in range(1, RETRY_ATTEMPTS + 1):
-- MAGIC         try:
-- MAGIC             throttle()
-- MAGIC             resp = requests.get(
-- MAGIC                 url,
-- MAGIC                 headers=get_headers(),
-- MAGIC                 params=params,
-- MAGIC                 timeout=REQUEST_TIMEOUT,
-- MAGIC                 verify=SSL_VERIFY,
-- MAGIC             )
-- MAGIC             resp.raise_for_status()
-- MAGIC             data = resp.json()
-- MAGIC             if not data.get("isSuccess"):
-- MAGIC                 raise ValueError(f"API error: {data.get('message')}")
-- MAGIC             return data["result"]
-- MAGIC         except (requests.RequestException, ValueError) as e:
-- MAGIC             print(f"  [WARN] Attempt {attempt}/{RETRY_ATTEMPTS} failed — {e}")
-- MAGIC             if attempt < RETRY_ATTEMPTS:
-- MAGIC                 time.sleep(RETRY_BACKOFF)
-- MAGIC             else:
-- MAGIC                 raise RuntimeError(f"All {RETRY_ATTEMPTS} attempts failed for {url}") from e
-- MAGIC
-- MAGIC
-- MAGIC def api_get_paged(url, list_key, page_size=100):
-- MAGIC     """
-- MAGIC     Fetch ALL pages for a paginated endpoint.
-- MAGIC
-- MAGIC     Tries common ReQtest pagination patterns:
-- MAGIC       1. ?page=N&pageSize=M  (1-based page index)
-- MAGIC       2. ?offset=N&limit=M   (offset-based)
-- MAGIC     Stops when a page returns fewer items than page_size.
-- MAGIC
-- MAGIC     Args:
-- MAGIC         url       : base endpoint URL (no query string)
-- MAGIC         list_key  : key inside result that holds the list, e.g. "testruns"
-- MAGIC         page_size : items per page (default 100; lower if the API caps earlier)
-- MAGIC
-- MAGIC     Returns:
-- MAGIC         list of all items across all pages (deduplicated by 'id' if present)
-- MAGIC     """
-- MAGIC     all_items = []
-- MAGIC     seen_ids  = set()
-- MAGIC     page      = 1
-- MAGIC
-- MAGIC     while True:
-- MAGIC         params = {"page": page, "pageSize": page_size}
-- MAGIC         result = api_get(url, params=params)
-- MAGIC
-- MAGIC         # result may be a list or a dict containing the list
-- MAGIC         if isinstance(result, list):
-- MAGIC             items = result
-- MAGIC         else:
-- MAGIC             items = (
-- MAGIC                 result.get(list_key)
-- MAGIC                 or result.get(list_key.replace("_", ""))   # camelCase fallback
-- MAGIC                 or []
-- MAGIC             )
-- MAGIC
-- MAGIC         if not items:
-- MAGIC             break   # no more data
-- MAGIC
-- MAGIC         # Deduplication guard — stops infinite loops if API ignores pagination params
-- MAGIC         new_items = []
-- MAGIC         for item in items:
-- MAGIC             item_id = item.get("id")
-- MAGIC             if item_id is None or item_id not in seen_ids:
-- MAGIC                 new_items.append(item)
-- MAGIC                 if item_id is not None:
-- MAGIC                     seen_ids.add(item_id)
-- MAGIC
-- MAGIC         all_items.extend(new_items)
-- MAGIC
-- MAGIC         if len(new_items) == 0:
-- MAGIC             # All items on this page were duplicates → definitely finished
-- MAGIC             print(f"    [PAGE] Stopping at page {page} — all items already seen (dedup guard).")
-- MAGIC             break
-- MAGIC
-- MAGIC         if len(items) < page_size:
-- MAGIC             break   # last page (partial)
-- MAGIC
-- MAGIC         page += 1
-- MAGIC
-- MAGIC     return all_items
-- MAGIC
-- MAGIC
-- MAGIC def test_connection():
-- MAGIC     """
-- MAGIC     Validate PAT and basic connectivity by calling the /projects endpoint
-- MAGIC     (lists all projects) — no specific project ID needed.
-- MAGIC     A 200 OR 403 both confirm the API is reachable and the PAT is recognised;
-- MAGIC     only a network error or a 401 'invalid token' response means the PAT is bad.
-- MAGIC     Per-project existence is checked separately inside the main loop.
-- MAGIC     Returns True/False.
-- MAGIC     """
-- MAGIC     print("[CHECK] Testing API connectivity via /projects list endpoint...")
-- MAGIC     try:
-- MAGIC         throttle()
-- MAGIC         resp = requests.get(
-- MAGIC             f"{API_BASE}/projects",
-- MAGIC             headers=get_headers(),
-- MAGIC             timeout=REQUEST_TIMEOUT,
-- MAGIC             verify=SSL_VERIFY,
-- MAGIC         )
-- MAGIC         # 200 = success, 403 = PAT valid but org-level list restricted (still reachable)
-- MAGIC         if resp.status_code in (200, 403):
-- MAGIC             print("[CHECK] ✓ API reachable and PAT is valid.\n")
-- MAGIC             return True
-- MAGIC         # 401 = PAT completely rejected — abort makes sense here
-- MAGIC         if resp.status_code == 401:
-- MAGIC             print(f"[CHECK] ✗ HTTP 401 — PAT is invalid or expired. Check your token.\n")
-- MAGIC             return False
-- MAGIC         # Any other unexpected status
-- MAGIC         print(f"[CHECK] ✗ HTTP {resp.status_code} — unexpected response.")
-- MAGIC         print(f"        Response: {resp.text[:300]}\n")
-- MAGIC         return False
-- MAGIC     except requests.ConnectionError as e:
-- MAGIC         print(f"[CHECK] ✗ Connection error — {e}")
-- MAGIC         if IS_DATABRICKS:
-- MAGIC             print("        Ensure outbound internet is enabled on your cluster.\n")
-- MAGIC         return False
-- MAGIC     except requests.Timeout:
-- MAGIC         print("[CHECK] ✗ Connection timed out.\n")
-- MAGIC         return False
-- MAGIC     except Exception as e:
-- MAGIC         print(f"[CHECK] ✗ Unexpected error — {e}\n")
-- MAGIC         return False
-- MAGIC
-- MAGIC
-- MAGIC def check_project_exists(project_id):
-- MAGIC     """
-- MAGIC     Check whether a specific project ID exists and is accessible.
-- MAGIC     Returns (exists: bool, reason: str).
-- MAGIC     Does NOT raise — safe to call inside the main loop.
-- MAGIC
-- MAGIC     Note: ReQtest returns HTTP 401 with error code 4000016 when a project
-- MAGIC     does not exist or the PAT lacks access to it — this is treated as
-- MAGIC     'missing/no access', NOT as an invalid PAT (which is caught in test_connection).
-- MAGIC     """
-- MAGIC     try:
-- MAGIC         throttle()
-- MAGIC         resp = requests.get(
-- MAGIC             f"{API_BASE}/projects/{project_id}",
-- MAGIC             headers=get_headers(),
-- MAGIC             timeout=REQUEST_TIMEOUT,
-- MAGIC             verify=SSL_VERIFY,
-- MAGIC         )
-- MAGIC         if resp.status_code == 200:
-- MAGIC             data = resp.json()
-- MAGIC             if not data.get("isSuccess", True):
-- MAGIC                 return False, f"API returned isSuccess=false: {data.get('message', 'unknown')}"
-- MAGIC             return True, "OK"
-- MAGIC         if resp.status_code == 404:
-- MAGIC             return False, "Project not found (404)"
-- MAGIC         if resp.status_code == 401:
-- MAGIC             # ReQtest uses 401 for "project not found or no access" (code 4000016)
-- MAGIC             # This is a per-project permission issue, NOT an invalid PAT
-- MAGIC             try:
-- MAGIC                 msg = resp.json().get("exception", {}).get("message", resp.text[:200])
-- MAGIC             except Exception:
-- MAGIC                 msg = resp.text[:200]
-- MAGIC             return False, f"No access or project does not exist (401): {msg}"
-- MAGIC         if resp.status_code == 403:
-- MAGIC             return False, f"Access denied (403) — PAT lacks permission for this project"
-- MAGIC         return False, f"Unexpected HTTP {resp.status_code}"
-- MAGIC     except requests.Timeout:
-- MAGIC         return False, "Request timed out"
-- MAGIC     except Exception as e:
-- MAGIC         return False, str(e)
-- MAGIC
-- MAGIC # ── FETCH FUNCTIONS ───────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC def fetch_testruns(project_id):
-- MAGIC     return api_get_paged(f"{API_BASE}/projects/{project_id}/testruns", list_key="testruns")
-- MAGIC
-- MAGIC
-- MAGIC def fetch_contents(project_id, testrun_id):
-- MAGIC     return api_get_paged(
-- MAGIC         f"{API_BASE}/projects/{project_id}/testruns/{testrun_id}/contents",
-- MAGIC         list_key="contents",
-- MAGIC     )
-- MAGIC
-- MAGIC
-- MAGIC def fetch_testcases(project_id):
-- MAGIC     return api_get_paged(f"{API_BASE}/projects/{project_id}/testcases", list_key="testcases")
-- MAGIC
-- MAGIC
-- MAGIC def fetch_testcase_detail(project_id, testcase_id):
-- MAGIC     """Single-item endpoint — no pagination needed."""
-- MAGIC     return api_get(f"{API_BASE}/projects/{project_id}/testcases/{testcase_id}")
-- MAGIC
-- MAGIC # ── ROW BUILDERS ──────────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC def build_testrun_row(project_id, tr):
-- MAGIC     return {
-- MAGIC         "project_id":         project_id,
-- MAGIC         "id":                 tr.get("id"),
-- MAGIC         "custom_id":          tr.get("customId"),
-- MAGIC         "type":               tr.get("type"),
-- MAGIC         "created_by":         tr.get("createdBy"),
-- MAGIC         "created_by_user_id": tr.get("createdByUserId"),
-- MAGIC         "archived":           tr.get("archived"),
-- MAGIC         "created_date":       tr.get("createdDate"),
-- MAGIC         "changed_date":       tr.get("changedDate"),
-- MAGIC     }
-- MAGIC
-- MAGIC
-- MAGIC def build_content_row(project_id, testrun_id, c):
-- MAGIC     return {
-- MAGIC         "project_id":          project_id,
-- MAGIC         "testrun_id":          testrun_id,
-- MAGIC         "id":                  c.get("id"),
-- MAGIC         "name":                c.get("name"),
-- MAGIC         "type":                c.get("type"),
-- MAGIC         "test_suite_id":       c.get("testSuiteId"),
-- MAGIC         "test_suite_name":     c.get("testSuiteName"),
-- MAGIC         "executed_by":         c.get("executedBy"),
-- MAGIC         "executed_by_user_id": c.get("executedByUserId"),
-- MAGIC         "execution_date":      c.get("executionDate"),
-- MAGIC         "result":              c.get("result"),
-- MAGIC         "result_text":         c.get("resultText"),
-- MAGIC         "pre_conditions":      c.get("preConditions"),
-- MAGIC         "test_case":           c.get("testCase"),
-- MAGIC         "links":               c.get("links"),
-- MAGIC     }
-- MAGIC
-- MAGIC
-- MAGIC def build_testcase_row(project_id, tc):
-- MAGIC     return {
-- MAGIC         "project_id":         project_id,
-- MAGIC         "id":                 tc.get("id"),
-- MAGIC         "custom_id":          tc.get("customId"),
-- MAGIC         "name":               tc.get("name"),
-- MAGIC         "type":               tc.get("type"),
-- MAGIC         "created_by":         tc.get("createdBy"),
-- MAGIC         "created_by_user_id": tc.get("createdByUserId"),
-- MAGIC         "archived":           tc.get("archived"),
-- MAGIC         "created_date":       tc.get("createdDate"),
-- MAGIC         "changed_date":       tc.get("changedDate"),
-- MAGIC         "test_suite_id":      tc.get("testSuiteId"),
-- MAGIC         "test_suite_name":    tc.get("testSuiteName"),
-- MAGIC     }
-- MAGIC
-- MAGIC
-- MAGIC def build_testcase_detail_row(project_id, testcase_id, detail):
-- MAGIC     return {
-- MAGIC         "project_id":         project_id,
-- MAGIC         "testcase_id":        testcase_id,
-- MAGIC         "id":                 detail.get("id"),
-- MAGIC         "custom_id":          detail.get("customId"),
-- MAGIC         "name":               detail.get("name"),
-- MAGIC         "type":               detail.get("type"),
-- MAGIC         "created_by":         detail.get("createdBy"),
-- MAGIC         "created_by_user_id": detail.get("createdByUserId"),
-- MAGIC         "archived":           detail.get("archived"),
-- MAGIC         "created_date":       detail.get("createdDate"),
-- MAGIC         "changed_date":       detail.get("changedDate"),
-- MAGIC         "pre_conditions":     detail.get("preConditions"),
-- MAGIC         "description":        detail.get("description"),
-- MAGIC         "expected_result":    detail.get("expectedResult"),
-- MAGIC         "test_steps":         detail.get("testSteps"),
-- MAGIC         "test_suite_id":      detail.get("testSuiteId"),
-- MAGIC         "test_suite_name":    detail.get("testSuiteName"),
-- MAGIC         "priority":           detail.get("priority"),
-- MAGIC         "status":             detail.get("status"),
-- MAGIC         "links":              detail.get("links"),
-- MAGIC         "tags":               detail.get("tags"),
-- MAGIC     }
-- MAGIC
-- MAGIC # ── MAIN ──────────────────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC def main():
-- MAGIC     if not PAT:
-- MAGIC         abort("[ERROR] PAT is not set. Add it to the CONFIGURATION section or set REQTEST_PAT env var.")
-- MAGIC     if not PROJECT_IDS or PROJECT_IDS == [""]:
-- MAGIC         abort("[ERROR] No project IDs configured.")
-- MAGIC
-- MAGIC     # ── Single connectivity + PAT check before entering the loop ─────────────
-- MAGIC     # if not test_connection():
-- MAGIC     #     abort("[ERROR] Cannot reach ReQtest API or PAT is invalid. Aborting.")
-- MAGIC
-- MAGIC     all_testruns  = []
-- MAGIC     all_contents  = []
-- MAGIC     all_testcases = []
-- MAGIC     all_tc_detail = []
-- MAGIC
-- MAGIC     # ── Project-level tracking ────────────────────────────────────────────────
-- MAGIC     projects_found   = []   # IDs that exist and were fetched successfully
-- MAGIC     projects_missing = []   # IDs that returned 404 / not found
-- MAGIC     projects_errored = []   # IDs that failed for other reasons (auth, timeout, etc.)
-- MAGIC
-- MAGIC     for project_id in PROJECT_IDS:
-- MAGIC         print(f"\n[PROJECT {project_id}] ────────────────────────────────")
-- MAGIC
-- MAGIC         # ── Per-project existence check (no abort) ────────────────────────────
-- MAGIC         exists, reason = check_project_exists(project_id)
-- MAGIC         if not exists:
-- MAGIC             if "404" in reason or "not found" in reason.lower():
-- MAGIC                 print(f"  [SKIP] Project {project_id} does not exist in the API — {reason}")
-- MAGIC                 projects_missing.append({"project_id": project_id, "reason": reason})
-- MAGIC             else:
-- MAGIC                 print(f"  [SKIP] Project {project_id} skipped due to error — {reason}")
-- MAGIC                 projects_errored.append({"project_id": project_id, "reason": reason})
-- MAGIC             continue   # move on to the next project; do NOT abort
-- MAGIC
-- MAGIC         projects_found.append(project_id)
-- MAGIC
-- MAGIC         # ── Testruns ──────────────────────────────────────────────────────────
-- MAGIC         try:
-- MAGIC             testruns = fetch_testruns(project_id)
-- MAGIC         except Exception as e:
-- MAGIC             print(f"  [ERROR] Could not fetch testruns: {e}")
-- MAGIC             testruns = []
-- MAGIC
-- MAGIC         print(f"  Found {len(testruns)} testrun(s).")
-- MAGIC
-- MAGIC         for tr in testruns:
-- MAGIC             all_testruns.append(build_testrun_row(project_id, tr))
-- MAGIC
-- MAGIC             testrun_id = tr.get("id")
-- MAGIC             print(f"  → Fetching contents for testrun {testrun_id}...")
-- MAGIC             try:
-- MAGIC                 contents = fetch_contents(project_id, testrun_id)
-- MAGIC             except Exception as e:
-- MAGIC                 print(f"    [ERROR] Could not fetch contents: {e}")
-- MAGIC                 contents = []
-- MAGIC
-- MAGIC             print(f"    Found {len(contents)} unique item(s).")
-- MAGIC             for c in contents:
-- MAGIC                 all_contents.append(build_content_row(project_id, testrun_id, c))
-- MAGIC
-- MAGIC         # ── Testcases list ────────────────────────────────────────────────────
-- MAGIC         print(f"\n  Fetching testcases for project {project_id}...")
-- MAGIC         try:
-- MAGIC             testcases = fetch_testcases(project_id)
-- MAGIC         except Exception as e:
-- MAGIC             print(f"  [ERROR] Could not fetch testcases: {e}")
-- MAGIC             testcases = []
-- MAGIC
-- MAGIC         print(f"  Found {len(testcases)} testcase(s).")
-- MAGIC
-- MAGIC         for tc in testcases:
-- MAGIC             all_testcases.append(build_testcase_row(project_id, tc))
-- MAGIC
-- MAGIC             testcase_id = tc.get("id")
-- MAGIC             print(f"  → Fetching detail for testcase {testcase_id}...")
-- MAGIC             try:
-- MAGIC                 detail = fetch_testcase_detail(project_id, testcase_id)
-- MAGIC                 all_tc_detail.append(build_testcase_detail_row(project_id, testcase_id, detail))
-- MAGIC             except Exception as e:
-- MAGIC                 print(f"    [ERROR] Could not fetch detail for testcase {testcase_id}: {e}")
-- MAGIC                 all_tc_detail.append({
-- MAGIC                     "project_id":  project_id,
-- MAGIC                     "testcase_id": testcase_id,
-- MAGIC                     "fetch_error": str(e),
-- MAGIC                 })
-- MAGIC
-- MAGIC     # ── Build DataFrames ──────────────────────────────────────────────────────
-- MAGIC     df_testruns        = pd.DataFrame(all_testruns)
-- MAGIC     df_contents        = pd.DataFrame(all_contents)
-- MAGIC     df_testcases       = pd.DataFrame(all_testcases)
-- MAGIC     df_testcase_detail = pd.DataFrame(all_tc_detail)
-- MAGIC
-- MAGIC     # ── Project availability summary ──────────────────────────────────────────
-- MAGIC     df_projects_found   = pd.DataFrame({"project_id": projects_found, "status": "found"})
-- MAGIC     df_projects_missing = pd.DataFrame(projects_missing).assign(status="missing") if projects_missing else pd.DataFrame(columns=["project_id", "reason", "status"])
-- MAGIC     df_projects_errored = pd.DataFrame(projects_errored).assign(status="error")   if projects_errored else pd.DataFrame(columns=["project_id", "reason", "status"])
-- MAGIC
-- MAGIC     print("\n" + "═" * 55)
-- MAGIC     print("[SUMMARY] Project Availability")
-- MAGIC     print("═" * 55)
-- MAGIC     print(f"  ✓ Found   ({len(projects_found)})  : {projects_found}")
-- MAGIC     if projects_missing:
-- MAGIC         print(f"  ✗ Missing ({len(projects_missing)}) :")
-- MAGIC         for m in projects_missing:
-- MAGIC             print(f"      {m['project_id']} — {m['reason']}")
-- MAGIC     if projects_errored:
-- MAGIC         print(f"  ⚠ Errored ({len(projects_errored)}) :")
-- MAGIC         for e in projects_errored:
-- MAGIC             print(f"      {e['project_id']} — {e['reason']}")
-- MAGIC     print("═" * 55)
-- MAGIC
-- MAGIC     print(f"\n[DONE] Testruns rows        : {len(df_testruns)}")
-- MAGIC     print(f"[DONE] Contents rows        : {len(df_contents)}")
-- MAGIC     print(f"[DONE] Testcases rows       : {len(df_testcases)}")
-- MAGIC     print(f"[DONE] Testcase Detail rows : {len(df_testcase_detail)}")
-- MAGIC
-- MAGIC     # ── Previews ──────────────────────────────────────────────────────────────
-- MAGIC     for label, df in [
-- MAGIC         ("Testruns",         df_testruns),
-- MAGIC         ("Testruns Content", df_contents),
-- MAGIC         ("Testcases",        df_testcases),
-- MAGIC         ("Testcase Details", df_testcase_detail),
-- MAGIC     ]:
-- MAGIC         if not df.empty:
-- MAGIC             print(f"\n── {label} (preview) ──────────────────────────────")
-- MAGIC             print(df.head(5).to_string(index=False))
-- MAGIC
-- MAGIC     
-- MAGIC
-- MAGIC     # ── Final merged DataFrame ────────────────────────────────────────────────
-- MAGIC     if not df_testruns.empty and not df_contents.empty:
-- MAGIC         df_testruns_slim = df_testruns[["project_id", "custom_id", "id"]]
-- MAGIC
-- MAGIC         df_contents_slim = df_contents[[
-- MAGIC             "testrun_id", "type", "id", "name", "result_text", "test_suite_id", "test_suite_name"
-- MAGIC         ]].rename(columns={
-- MAGIC             "testrun_id":      "Test Run ID",
-- MAGIC             "test_suite_name": "Test Suite Name",
-- MAGIC             "type":            "Type",
-- MAGIC             "id":              "Test Run Content ID",
-- MAGIC             "test_suite_id":   "Test Suite ID",
-- MAGIC             "name":            "Test Run Name",
-- MAGIC             "result_text":     "Execution Result",
-- MAGIC         })
-- MAGIC
-- MAGIC         df_reqtest = (
-- MAGIC             df_testruns_slim
-- MAGIC             .merge(df_contents_slim, left_on="id", right_on="Test Run ID", how="inner")
-- MAGIC             .rename(columns={"project_id": "Project ID", "custom_id": "Custom ID"})
-- MAGIC             [["Project ID", "Custom ID", "Test Run ID", "Test Suite Name",
-- MAGIC               "Type", "Test Run Content ID", "Test Suite ID", "Test Run Name", "Execution Result"]]
-- MAGIC         )
-- MAGIC
-- MAGIC         print(f"\n[MERGED] df_reqtest rows: {len(df_reqtest)}")
-- MAGIC         print(df_reqtest.head(5).to_string(index=False))
-- MAGIC     else:
-- MAGIC         df_reqtest = pd.DataFrame()
-- MAGIC         print("\n[MERGED] Skipped — testruns or contents DataFrame is empty.")
-- MAGIC
-- MAGIC     return (
-- MAGIC         df_testruns, df_contents, df_testcases, df_testcase_detail, df_reqtest,
-- MAGIC         projects_found, projects_missing, projects_errored,
-- MAGIC     )
-- MAGIC
-- MAGIC # ── ENTRY POINT ───────────────────────────────────────────────────────────────
-- MAGIC
-- MAGIC (
-- MAGIC     df_testruns, df_contents, df_testcases, df_testcase_detail, df_reqtest,
-- MAGIC     projects_found, projects_missing, projects_errored,
-- MAGIC ) = main()

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC df_reqtest.display()

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC %python
-- MAGIC df_reqtest = df_reqtest.rename(columns={
-- MAGIC     "Project ID":          "project_id",
-- MAGIC     "Custom ID":           "custom_id",
-- MAGIC     "Test Run ID":         "test_run_id",
-- MAGIC     "Test Suite Name":      "test_suite_name",
-- MAGIC     "Type":                "type",
-- MAGIC     "Test Run Content ID": "test_run_content_id",
-- MAGIC     "Test Suite ID":       "test_suite_id",
-- MAGIC     "Test Run Name":        "test_run_name",
-- MAGIC     "Execution Result":    "execution_result",
-- MAGIC })

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read directly from the existing table
-- MAGIC vw_reqtest_src = spark.table("qa_wb.saasfactory.g_fct_cpm_reqtest")

-- COMMAND ----------


MERGE INTO qa_wb.saasfactory.g_fct_cpm_reqtest_hist_new t
USING vw_reqtest_src s
ON t.test_run_content_id = s.test_run_content_id
AND t.is_current = true

WHEN MATCHED AND (
       COALESCE(t.project_id,'') <> COALESCE(s.project_id,'')
    OR COALESCE(t.custom_id,-1) <> COALESCE(s.custom_id,-1)
    OR COALESCE(t.test_run_id,-1) <> COALESCE(s.test_run_id,-1)
    OR COALESCE(t.test_suite_name,'') <> COALESCE(s.test_suite_name,'')
    OR COALESCE(t.type,'') <> COALESCE(s.type,'')
    OR COALESCE(t.test_suite_id,-1) <> COALESCE(s.test_suite_id,-1)
    OR COALESCE(t.test_run_name,'') <> COALESCE(s.test_run_name,'')
    OR COALESCE(t.execution_result,'') <> COALESCE(s.execution_result,'')
)
THEN UPDATE SET
    t.is_current = false;

-- COMMAND ----------

INSERT INTO qa_wb.saasfactory.g_fct_cpm_reqtest_hist_new
(
    project_id,
    custom_id,
    test_run_id,
    test_suite_name,
    type,
    test_run_content_id,
    test_suite_id,
    test_run_name,
    execution_result,
    is_current,
    load_date
)
SELECT
    s.project_id,
    s.custom_id,
    s.test_run_id,
    s.test_suite_name,
    s.type,
    s.test_run_content_id,
    s.test_suite_id,
    s.test_run_name,
    s.execution_result,
    true,
    current_date()
FROM vw_reqtest_src s
LEFT JOIN qa_wb.saasfactory.g_fct_cpm_reqtest_hist_new t
    ON s.test_run_content_id = t.test_run_content_id
    AND t.is_current = true
WHERE
    t.test_run_content_id IS NULL

    OR

    (
           COALESCE(t.project_id,'') <> COALESCE(s.project_id,'')
        OR COALESCE(t.custom_id,-1) <> COALESCE(s.custom_id,-1)
        OR COALESCE(t.test_run_id,-1) <> COALESCE(s.test_run_id,-1)
        OR COALESCE(t.test_suite_name,'') <> COALESCE(s.test_suite_name,'')
        OR COALESCE(t.type,'') <> COALESCE(s.type,'')
        OR COALESCE(t.test_suite_id,-1) <> COALESCE(s.test_suite_id,-1)
        OR COALESCE(t.test_run_name,'') <> COALESCE(s.test_run_name,'')
        OR COALESCE(t.execution_result,'') <> COALESCE(s.execution_result,'')
    );

-- COMMAND ----------

-- MAGIC %skip
-- MAGIC select * from qa_wb.saasfactory.g_fct_cpm_reqtest_hist_new where load_date=current_date()