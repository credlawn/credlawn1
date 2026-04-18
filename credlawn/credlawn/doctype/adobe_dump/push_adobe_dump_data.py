import frappe
from frappe import _
from frappe.utils import now_datetime, get_datetime, add_days, getdate
import requests
import pytz
from datetime import datetime

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
        
        # Task 3: Sync Activation Data (BPA Source)
        publish_progress(67, _("Task 2 Complete. Starting Task 3: Activation Sync..."))
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
    """VKYC sync logic (Upsert matched by ARN)."""
    pb_url = frappe.conf.get("pocketbase_url")
    pb_token = frappe.conf.get("pocketbase_auth_token")

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
    Task 3: Optimized Activation Sync (BPA Source)
    - Ranking-based comparison of two status fields.
    - Conditional Creation (Rank 1 & 2) vs Update Only (Rank 3).
    - Global latest Adobe Dump date as status date.
    """
    pb_url = frappe.conf.get("pocketbase_url")
    pb_token = frappe.conf.get("pocketbase_auth_token")

    # 1. Fetch Shared Global Data
    latest_adobe_date = frappe.db.get_value("Adobe Dump", {}, "adobe_dump_date", order_by="adobe_dump_date desc")
    status_date_str = f"{getdate(latest_adobe_date)} 12:00:00.000Z" if latest_adobe_date else None

    # 2. Fetch BPA Records
    records = frappe.db.get_all("BPA Records", fields=[
        "employee_name", "employee_code", "customer_name", "mobile_no", 
        "arn_no", "decision_month", "decision_date", 
        "card_activation_status", "activation_status"
    ])

    if not records: return {"count": 0}

    # 3. Status Ranking Logic
    def get_status_rank(val):
        if not val: return 0
        s = str(val).strip().upper().replace(" ", "").replace("+", "")
        if "TXNACTIVE" in s: return 3
        if "VACTIVE" in s: return 2
        if "INACTIVE" in s: return 1
        return 0

    canonical_map = {3: "Txn Active", 2: "V+ Active", 1: "Inactive"}

    # 4. Pocketbase ID Mapping
    pb_records = fetch_pocketbase_ids(pb_url, pb_token, "activation")
    arn_to_pb_id = {r.get("arn_no"): r.get("id") for r in pb_records if r.get("arn_no")}

    # 5. Process Matrix
    batch_requests = []
    today_dt = getdate()
    # Aging Thresholds
    inactive_threshold = add_days(today_dt, -37)
    vactive_threshold = add_days(today_dt, -120)

    for doc in records:
        rank_a = get_status_rank(doc.card_activation_status)
        rank_b = get_status_rank(doc.activation_status)
        max_rank = max(rank_a, rank_b)

        if max_rank == 0: continue # Skip Rank 0 completely
        
        final_status = canonical_map[max_rank]
        pb_id = arn_to_pb_id.get(doc.arn_no)

        # Aging Check (Only for creation/standard updates)
        # Note: Rank 3 (Txn Active) bypasses aging to ensure cleanup
        if max_rank != 3:
            dec_date = getdate(doc.decision_date) if doc.decision_date else None
            if not dec_date: continue
            if max_rank == 1 and dec_date < inactive_threshold: continue
            if max_rank == 2 and dec_date < vactive_threshold: continue

        # Action Matrix Rules
        method = "PATCH" if pb_id else "POST"
        
        # Rank 3 Rule: ONLY Update (Skip if not in PB)
        if max_rank == 3 and not pb_id:
            continue
            
        payload = {
            "employee_name": doc.employee_name,
            "employee_code": doc.employee_code,
            "customer_name": doc.customer_name,
            "mobile_no": doc.mobile_no,
            "arn_no": doc.arn_no,
            "decision_month": doc.decision_month,
            "decision_date": f"{getdate(doc.decision_date)} 12:00:00.000Z" if doc.decision_date else None,
            "bank_status_date": status_date_str,
            "bank_status": final_status,
            "remove_data": True if max_rank == 3 else False
        }

        batch_requests.append({
            "method": method,
            "url": f"/api/collections/activation/records/{pb_id}" if pb_id else "/api/collections/activation/records",
            "body": payload
        })

    if batch_requests:
        send_pb_batch(pb_url, pb_token, batch_requests)
    
    return {"count": len(batch_requests)}

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
