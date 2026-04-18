import frappe
from frappe import _
from frappe.utils import now_datetime, get_datetime, add_days, getdate
import requests
import pytz
from datetime import datetime
import re

@frappe.whitelist()
def execute_push():
    """
    Entry point for sequential data push.
    Encapsulates sequential tasks with real-time progress reporting.
    """
    try:
        # Task 1: Create VKYC Records
        publish_progress(5, _("Starting Task 1: VKYC Push..."))
        vkyc_results = task_1_create_vkyc_record()
        
        # Task 2: Update BKYC Data
        publish_progress(34, _("Task 1 Complete. Starting Task 2: BKYC Push..."))
        bkyc_results = task_2_update_bkyc_data()
        
        # Task 3: Sync Activation Data
        publish_progress(67, _("Task 2 Complete. Starting Task 3: Activation Push..."))
        activation_results = task_3_sync_activation_data()
        
        # Final Summary
        summary = _("Push Complete! Task 1 (VKYC): {0}, Task 2 (BKYC): {1}, Task 3 (Activation): {2} processed.").format(
            vkyc_results.get("count", 0),
            bkyc_results.get("count", 0),
            activation_results.get("count", 0)
        )
        publish_progress(100, summary)
        
        return {"status": "success", "message": summary}

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Adobe Dump Push Critical Failure")
        publish_progress(0, _("Push failed: {0}").format(str(e)), failed=True)
        return {"status": "error", "message": str(e)}

def task_1_create_vkyc_record():
    """VKYC sync logic (Upsert at 17:00 IST/converted to UTC)."""
    pb_url = frappe.conf.get("pocketbase_url")
    pb_token = frappe.conf.get("pocketbase_auth_token")
    if not pb_url or not pb_token:
        frappe.throw(_("Pocketbase connection details missing."))

    today_start = f"{getdate()} 00:00:00"
    records = frappe.db.sql("""
        SELECT employee_name, employee_code, arn_no, customer_name, 
               mobile_no, vkyc_expiry_date, vkyc_link, vkyc_status
        FROM `tabAdobe Dump`
        WHERE vkyc_expiry_date >= %s 
        AND (vkyc_status IS NULL OR vkyc_status = '' OR LOWER(vkyc_status) LIKE '%%incomplete%%')
    """, (today_start,), as_dict=True)

    if not records: return {"count": 0}

    pb_records = fetch_pocketbase_ids(pb_url, pb_token, "vkyc")
    arn_to_pb_id = {r.get("arn_no"): r.get("id") for r in pb_records if r.get("arn_no")}

    batch_requests = []
    for doc in records:
        pb_id = arn_to_pb_id.get(doc.arn_no)
        batch_requests.append({
            "method": "PATCH" if pb_id else "POST",
            "url": f"/api/collections/vkyc/records/{pb_id}" if pb_id else "/api/collections/vkyc/records",
            "body": {
                "employee_name": doc.employee_name,
                "employee_code": doc.employee_code,
                "arn_no": doc.arn_no,
                "bank_vkyc_status": "Pending",
                "customer_name": doc.customer_name,
                "mobile_no": doc.mobile_no,
                "vkyc_expiry_date": ist_to_pb_utc(doc.vkyc_expiry_date),
                "vkyc_link": doc.vkyc_link
            }
        })
    send_pb_batch(pb_url, pb_token, batch_requests)
    return {"count": len(records)}

def task_2_update_bkyc_data():
    """BKYC sync logic (Skip if exist, 60-day filter)."""
    pb_url = frappe.conf.get("pocketbase_url")
    pb_token = frappe.conf.get("pocketbase_auth_token")

    date_threshold = add_days(getdate(), -60)
    records = frappe.db.sql("""
        SELECT employee_name, employee_code, arn_no, customer_name, 
               mobile_no, bkyc_reason
        FROM `tabAdobe Dump`
        WHERE (LOWER(bkyc_reason) LIKE '%%contact%%' OR LOWER(bkyc_reason) LIKE '%%decline%%')
        AND (decline_code IS NULL OR decline_code = '')
        AND (employee_name IS NOT NULL AND employee_name != '' AND employee_name != 'Unmapped')
        AND arn_date >= %s
    """, (date_threshold,), as_dict=True)

    if not records: return {"count": 0}

    pb_records = fetch_pocketbase_ids(pb_url, pb_token, "bkyc")
    existing_arns = set([r.get("arn_no") for r in pb_records if r.get("arn_no")])

    batch_requests = []
    for doc in records:
        if doc.arn_no in existing_arns: continue
        batch_requests.append({
            "method": "POST",
            "url": "/api/collections/bkyc/records",
            "body": {
                "employee_name": doc.employee_name,
                "employee_code": doc.employee_code,
                "customer_name": doc.customer_name,
                "mobile_no": doc.mobile_no,
                "arn_no": doc.arn_no,
                "bank_status": "Pending",
                "bank_remarks": doc.bkyc_reason
            }
        })
    if batch_requests:
        send_pb_batch(pb_url, pb_token, batch_requests)
    return {"count": len(batch_requests)}

def task_3_sync_activation_data():
    """
    Task 3: Activation Sync
    - Filter: APPROVE decision + specific activation status.
    - Match by arn_no (Upsert).
    - Time: Forced 12:00:00Z UTC.
    """
    pb_url = frappe.conf.get("pocketbase_url")
    pb_token = frappe.conf.get("pocketbase_auth_token")

    # 1. Fetch Approved Records
    records = frappe.db.sql("""
        SELECT employee_name, employee_code, arn_no, customer_name, 
               mobile_no, decision_month, final_decision_date,
               adobe_dump_date, card_activation_status
        FROM `tabAdobe Dump`
        WHERE LOWER(final_decision) LIKE '%%approve%%'
    """, as_dict=True)

    if not records: return {"count": 0}

    # 2. Status Normalization & Filtering
    def get_normalized_status(status):
        if not status: return None
        s = str(status).strip().upper().replace(" ", "").replace("+", "")
        if "INACTIVE" in s: return "Inactive"
        if "VACTIVE" in s: return "V+ Active"
        return None

    # Filter records by normalized status
    eligible_records = []
    for r in records:
        norm_status = get_normalized_status(r.card_activation_status)
        if norm_status:
            r.normalized_status = norm_status
            eligible_records.append(r)

    if not eligible_records: return {"count": 0}

    # 3. Pocketbase ID Mapping (Upsert Matching by arn_no)
    pb_records = fetch_pocketbase_ids(pb_url, pb_token, "activation")
    arn_to_pb_id = {r.get("arn_no"): r.get("id") for r in pb_records if r.get("arn_no")}

    # 4. Prepare Batch Requests
    batch_requests = []
    for doc in eligible_records:
        pb_id = arn_to_pb_id.get(doc.arn_no)
        
        # User requested 12:00:00 UTC time string specifically
        def to_utc_12(date_val):
            if not date_val: return None
            return f"{getdate(date_val)} 12:00:00.000Z"

        payload = {
            "employee_name": doc.employee_name,
            "employee_code": doc.employee_code,
            "customer_name": doc.customer_name,
            "mobile_no": doc.mobile_no,
            "arn_no": doc.arn_no,
            "decision_month": doc.decision_month,
            "decision_date": to_utc_12(doc.final_decision_date),
            "bank_status_date": to_utc_12(doc.adobe_dump_date),
            "bank_status": doc.normalized_status
        }

        batch_requests.append({
            "method": "PATCH" if pb_id else "POST",
            "url": f"/api/collections/activation/records/{pb_id}" if pb_id else "/api/collections/activation/records",
            "body": payload
        })

    if batch_requests:
        send_pb_batch(pb_url, pb_token, batch_requests)
    
    return {"count": len(eligible_records)}

def fetch_pocketbase_ids(pb_url, pb_token, collection):
    """Retrieves all ids and arn_nos from a collection."""
    all_items = []
    page = 1
    per_page = 500
    while True:
        api_url = f"{pb_url.rstrip('/')}/api/collections/{collection}/records"
        params = {"fields": "id,arn_no", "perPage": per_page, "page": page}
        try:
            res = requests.get(api_url, headers={"Authorization": f"Bearer {pb_token}"}, params=params, timeout=30)
            if res.status_code == 200:
                data = res.json()
                items = data.get("items", [])
                all_items.extend(items)
                if len(items) < per_page: break
                page += 1
            else: break
        except: break
    return all_items

def send_pb_batch(pb_url, pb_token, all_requests, chunk_size=50):
    """Sends requests to Pocketbase using the Batch API."""
    api_url = f"{pb_url.rstrip('/')}/api/batch"
    headers = {"Authorization": f"Bearer {pb_token}", "Content-Type": "application/json"}
    for i in range(0, len(all_requests), chunk_size):
        chunk = all_requests[i:i + chunk_size]
        try:
            res = requests.post(api_url, headers=headers, json={"requests": chunk}, timeout=60)
            if res.status_code != 200:
                frappe.log_error(f"PB Batch Fail: {res.text}", "Adobe Dump Push")
        except Exception as e:
            frappe.log_error(f"PB Batch Fail: {str(e)}", "Adobe Dump Push")

def ist_to_pb_utc(dt):
    """IST to PB UTC converter (used for VKYC/Task 1)."""
    if not dt: return None
    try:
        dt_obj = get_datetime(dt)
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_dt = ist_tz.localize(dt_obj)
        utc_dt = ist_dt.astimezone(pytz.UTC)
        return utc_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"
    except: return None

def publish_progress(percentage, message, failed=False):
    """ Websocket progress reporter."""
    frappe.publish_realtime("adobe_dump_push_progress", {
        "percentage": percentage, "message": message, "failed": failed
    })
