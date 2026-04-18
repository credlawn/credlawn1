import frappe
from frappe import _
import requests
from datetime import datetime
import pytz
from frappe.utils import now_datetime

@frappe.whitelist()
def execute_sync():
    """Main entry point for Turbo Call Log Sync"""
    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.call_log.sync_call_log.sync_job',
            queue='long',
            timeout=7200,
            is_async=True,
            job_name='call_log_sync'
        )
        return _("Turbo Call Log Sync started in the background.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Call Log Sync Enqueue Error")
        return _("Failed to start sync: {0}").format(str(e))

def sync_job():
    """Turbo background job using 1000 Batch Size and SQL Upsert"""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        # Checkpoint: Latest call record synced
        latest_pb_created = frappe.db.sql("SELECT MAX(pb_created) FROM `tabCall Log`")[0][0]

        pb_filter = ""
        if latest_pb_created:
            pb_filter = f"created > '{latest_pb_created}'"

        total_fetched = 0
        success_count = 0
        batch_size = 1000 # Increased to 1000 for maximum throughput
        page = 1
        collection_name = "call_logs"
        
        initial_res = fetch_pb_records(pb_url, pb_token, collection_name, 1, 1, pb_filter, return_raw=True)
        total_items = initial_res.get('totalItems', 0) if initial_res else 0

        if total_items == 0:
            publish_progress(100, "Call Logs are already up-to-date.")
            return

        while True:
            records = fetch_pb_records(pb_url, pb_token, collection_name, page, batch_size, pb_filter)
            if not records: break

            try:
                perform_upsert_batch(records)
                success_count += len(records)
            except Exception as e:
                # Springback mechanism: Process one-by-one if batch fails
                frappe.log_error(f"Call Log Batch {page} failed. Using fallback: {str(e)}", "Call Log Sync Fallback")
                for rec in records:
                    try:
                        perform_upsert_batch([rec])
                        success_count += 1
                    except: pass

            total_fetched += len(records)
            
            # Progress update
            progress = int((total_fetched / total_items) * 100)
            publish_progress(progress, f"Syncing Logs: {total_fetched} / {total_items}...")

            if len(records) < batch_size: break
            page += 1
            frappe.db.commit()

        frappe.db.commit()
        publish_progress(100, f"Sync Complete! {success_count} logs ingested.")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Call Log Sync Fatal Error")
        publish_progress(0, "Sync Failed. Check Logs.", failed=True)

def perform_upsert_batch(records):
    """High-speed Upsert for Call Logs"""
    if not records: return

    now_val = now_datetime()
    user = "Administrator"
    
    fields = [
        'name', 'creation', 'modified', 'owner', 'modified_by', 'docstatus',
        'employee_name', 'employee_code', 'mobile_no', 'call_date', 'call_time',
        'call_duration', 'ring_duration', 'session_duration', 'call_type', 'call_status',
        'pb_id', 'pb_created', 'pb_updated', 'clean_from_pb', 'deleted_from_pb'
    ]

    values = []
    for r in records:
        pb_id = r.get('id')
        ist_dt = convert_to_ist(r.get('call_timestamp'))
        
        values.append((
            pb_id, now_val, now_val, user, user, 0,
            r.get('employee_name'),
            r.get('employee_code'),
            r.get('phone_number'),
            ist_dt.date() if ist_dt else None,
            ist_dt.strftime("%H:%M:%S") if ist_dt else None,
            r.get('call_duration') or 0,
            r.get('ring_duration') or 0,
            r.get('session_duration') or 0,
            r.get('call_type'),
            r.get('call_status'),
            pb_id,
            r.get('created'), # Raw
            r.get('updated'), # Raw
            (1 if r.get('clean_from_pb') else 0),
            (1 if r.get('deleted_from_pb') else 0)
        ))

    placeholders = "(" + ", ".join(["%s"] * len(fields)) + ")"
    placeholders_list = ", ".join([placeholders] * len(values))
    
    # Update all fields except system keys on duplicate
    update_parts = [f"`{f}`=VALUES(`{f}`)" for f in fields if f not in ['name', 'creation', 'owner', 'pb_id', 'pb_created']]
    
    query = f"""
        INSERT INTO `tabCall Log` ({', '.join(['`' + f + '`' for f in fields])}) 
        VALUES {placeholders_list}
        ON DUPLICATE KEY UPDATE {', '.join(update_parts)}
    """
    
    frappe.db.sql(query, [val for row in values for val in row])

def convert_to_ist(utc_str):
    if not utc_str: return None
    try:
        utc_str = str(utc_str).replace(' ', 'T')
        if utc_str.endswith('Z'): utc_str = utc_str[:-1]
        dt = datetime.fromisoformat(utc_str)
        ist = dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata'))
        return ist.replace(tzinfo=None)
    except: return None

def fetch_pb_records(pb_url, pb_token, collection, page, per_page, pb_filter="", return_raw=False):
    try:
        url = f"{pb_url.rstrip('/')}/api/collections/{collection}/records"
        params = {"page": page, "perPage": per_page, "sort": "+created", "filter": f"({pb_filter})" if pb_filter else ""}
        res = requests.get(url, headers={"Authorization": f"Bearer {pb_token}"}, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            return data if return_raw else data.get('items', [])
        return {} if return_raw else []
    except: return {} if return_raw else []

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("call_log_sync_progress", {"percentage": percentage, "message": message, "failed": failed})
