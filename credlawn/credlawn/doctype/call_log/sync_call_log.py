import frappe
from frappe import _
import requests
from datetime import datetime
import pytz
from frappe.utils import now_datetime

@frappe.whitelist()
def execute_sync():
    """Main entry point for High-Speed Call Log Sync"""
    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.call_log.sync_call_log.sync_job',
            queue='long',
            timeout=7200,
            is_async=True,
            job_name='call_log_sync'
        )
        return _("High-Speed Call Log Sync started in the background.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Call Log Sync Enqueue Error")
        return _("Failed to start sync: {0}").format(str(e))

def sync_job():
    """Optimized background job using Timestamp Checkpoint and Batch SQL Insert"""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        # Step 1: Automatic Checkpoint - Find the latest pb_created in Frappe
        latest_pb_created = frappe.db.get_value("Call Log", 
            filters={"pb_id": ["is", "set"]}, 
            fieldname="pb_created", 
            order_by="pb_created desc"
        )

        # Step 2: Prepare Filtering
        pb_filter = ""
        if latest_pb_created:
            pb_filter = f"created > '{latest_pb_created}'"

        total_fetched = 0
        inserted_count = 0
        failed_count = 0
        batch_size = 500
        page = 1
        collection_name = "call_logs"
        
        # Initial call to get total record count for the progress bar
        initial_res = fetch_pb_records(pb_url, pb_token, collection_name, 1, 1, pb_filter, return_raw=True)
        total_items = initial_res.get('totalItems', 0) if initial_res else 0

        while True:
            records = fetch_pb_records(pb_url, pb_token, collection_name, page, batch_size, pb_filter)
            if not records:
                break

            # Step 3: Fast Batch SQL Insert
            try:
                success = perform_batch_insert(records)
                if success:
                    inserted_count += len(records)
                else:
                    failed_count += len(records)
            except Exception as e:
                failed_count += len(records)
                frappe.log_error(f"Batch Insert Error: {str(e)}", "Call Log Sync")

            total_fetched += len(records)
            
            # Progress update
            if total_items > 0:
                progress = int((total_fetched / total_items) * 100)
                publish_progress(progress, f"Ingested {total_fetched} of {total_items}...")

            if len(records) < batch_size:
                break
            
            # We don't increment page because 'created > latest' always fetches the next set
            # if we use the NEW latest from the current batch.
            # But the filter is static for this JOB. So we MUST use normal pagination.
            page += 1
            frappe.db.commit()

        frappe.db.commit()
        summary = f"High-Speed Sync Complete! Ingested: {inserted_count}, Failed: {failed_count}"
        publish_progress(100, summary)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Call Log Sync Job Fatal Error")
        publish_progress(0, "Sync Failed. Check Error Logs.", failed=True)

def fetch_pb_records(pb_url, pb_token, collection, page, per_page, pb_filter="", return_raw=False):
    try:
        url = f"{pb_url.rstrip('/')}/api/collections/{collection}/records"
        headers = {"Authorization": f"Bearer {pb_token}"}
        params = {
            "page": page, 
            "perPage": per_page,
            "sort": "+created" # Ascending order for sequential sync
        }
        if pb_filter:
            params["filter"] = f"({pb_filter})"
            
        res = requests.get(url, headers=headers, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            return data if return_raw else data.get('items', [])
        return {} if return_raw else []
    except Exception:
        return {} if return_raw else []

def perform_batch_insert(records):
    """Inserts a batch of records directly into the database using optimized SQL"""
    if not records:
        return True

    now_val = now_datetime()
    user = frappe.session.user or "Administrator"
    
    # Field mapping and value preparation
    values = []
    for r in records:
        ts = r.get('call_timestamp')
        ist_dt = convert_to_ist(ts)
        
        call_date = ist_dt.date() if ist_dt else None
        call_time = ist_dt.strftime("%H:%M:%S") if ist_dt else None
        
        # We use PB ID as the record name for uniqueness and speed
        name = r.get('id')
        
        values.append((
            name, now_val, now_val, user, user, 0, # Standard fields
            r.get('employee_name'),
            r.get('employee_code'),
            r.get('phone_number'), # Map to mobile_no
            call_date,
            call_time,
            r.get('call_duration'),
            r.get('ring_duration'),
            r.get('session_duration'),
            r.get('call_type'),
            r.get('call_status'),
            r.get('id'), # pb_id
            r.get('created'), # pb_created
            r.get('updated'), # pb_updated
            0, # clean_from_pb
            0  # deleted_from_pb
        ))

    # Correct Multi-row Insert Syntax for Frappe/MySQL
    # We have 21 columns
    placeholders = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    placeholders_list = ", ".join([placeholders] * len(values))
    
    # Flatten the values list
    flattened_values = [val for row in values for val in row]

    query = f"""
        INSERT IGNORE INTO `tabCall Log` (
            name, creation, modified, owner, modified_by, docstatus,
            employee_name, employee_code, mobile_no, call_date, call_time,
            call_duration, ring_duration, session_duration, call_type, call_status,
            pb_id, pb_created, pb_updated, clean_from_pb, deleted_from_pb
        ) VALUES {placeholders_list}
    """
    
    frappe.db.sql(query, flattened_values)
    return True

def convert_to_ist(utc_str):
    try:
        utc_str = str(utc_str).replace(' ', 'T')
        if utc_str.endswith('Z'): utc_str = utc_str[:-1]
        dt = datetime.fromisoformat(utc_str)
        ist = dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata'))
        return ist.replace(tzinfo=None)
    except Exception: return None

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("call_log_sync_progress", {"percentage": percentage, "message": message, "failed": failed})
