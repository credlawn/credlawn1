import frappe
import requests
from frappe import _
from datetime import datetime
import pytz
from frappe.utils import now_datetime

@frappe.whitelist()
def execute_sync():
    """Main entry point for Active Database sync. Enqueues a background job."""
    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.active_database.sync_active_database.sync_job',
            queue='long',
            timeout=7200,
            is_async=True,
            job_name='active_database_sync'
        )
        return _("Turbo Active Database Sync started. Check progress bar.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Database Sync Enqueue Error")
        return _("Failed to start sync: {0}").format(str(e))

def sync_job():
    """Optimized background job using High-Performance Bulk Upsert."""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            frappe.log_error("PocketBase credentials missing", "Active Database Sync Error")
            return

        # Checkpoint: Fetch max pb_updated from local records to pick up where we left off
        latest_pb_updated = frappe.db.sql("SELECT MAX(pb_updated) FROM `tabActive Database`")[0][0]

        pb_filter = ""
        if latest_pb_updated:
            pb_filter = f"updated > '{latest_pb_updated}'"

        page = 1
        per_page = 1000 # Optimized batch size
        collection_name = "pbc_2300403255" 
        
        total_fetched = 0
        success_count = 0
        
        # Initial call to get totalItems
        initial_res = fetch_pb_records(pb_url, pb_token, collection_name, 1, 1, pb_filter, return_raw=True)
        total_items = initial_res.get('totalItems', 0) if initial_res else 0

        if total_items == 0:
            publish_progress(100, "Local database is already up-to-date.")
            return

        while True:
            records = fetch_pb_records(pb_url, pb_token, collection_name, page, per_page, pb_filter)
            if not records: break

            # Process 1000 records in a single high-speed SQL call
            try:
                perform_upsert_batch(records)
                success_count += len(records)
            except Exception as e:
                # FALLBACK: If batch fails, try one-by-one to isolate the error and save the rest
                frappe.log_error(f"Batch {page} failed. Entering retry mode: {str(e)}", "Sync Fallback Initiated")
                for rec in records:
                    try:
                        perform_upsert_batch([rec])
                        success_count += 1
                    except:
                        pass # Skipping only the individual malformed record

            total_fetched += len(records)
            
            # Update Progress
            progress = int((total_fetched / total_items) * 100)
            publish_progress(progress, f"Processing: {total_fetched} / {total_items}...")

            if len(records) < per_page: break
            
            page += 1
            frappe.db.commit()

        frappe.db.commit()
        summary = f"Turbo Sync Complete! Total: {success_count} records synchronized."
        publish_progress(100, summary)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Database Sync Fatal Error")
        publish_progress(0, "Sync Failed. Check Error Log.", failed=True)

def perform_upsert_batch(records):
    """
    Executes a high-speed 'INSERT ... ON DUPLICATE KEY UPDATE' query.
    This handles both Insert and Update in a single atomic database operation.
    """
    if not records: return

    # 1. Define Fields
    fields = [
        'name', 'creation', 'modified', 'owner', 'modified_by', 'docstatus',
        'customer_name', 'mobile_no', 'city', 'segment', 'employer',
        'decline_reason', 'product', 'old_arn_no', 'old_decision_date',
        'promo_code', 'import_date', 'data_code', 'data_sub_code', 'custom_code',
        'allocation_count', 'employee_count', 'data_status', 'total_calls',
        'connected_calls', 'connected_duration', 'last_shuffle_date', 
        'shuffle_count', 'lead_status', 'lead_status_date', 'lead_status_time',
        'connected_employee', 'employee_name', 'employee_code', 'no_reallocation',
        'pb_id', 'pb_created', 'pb_updated'
    ]

    now_val = now_datetime()
    values = []

    for pb in records:
        pb_id = pb.get('id')
        lead_date, lead_time = convert_dt(pb.get('lead_status_date'), both=True)
        old_dec_date = convert_dt(pb.get('old_decision_date'))
        import_date = convert_dt(pb.get('import_date'))
        last_shuffle = convert_dt(pb.get('last_shuffle_date'))
        
        # Prepare 38-tuple for each record
        values.append((
            pb_id, # name
            now_val, # creation (Use local time, standard Frappe way)
            now_val, # modified
            'Administrator', # owner
            'Administrator', # modified_by
            0, # docstatus
            pb.get('customer_name'), pb.get('mobile_no'), pb.get('city'), pb.get('segment'), pb.get('employer'),
            pb.get('decline_reason'), pb.get('product'), pb.get('old_arn_no'), old_dec_date,
            pb.get('promo_code'), import_date, pb.get('data_code'), pb.get('data_sub_code'), pb.get('custom_code'),
            pb.get('allocation_count') or 0, pb.get('employee_count') or 0, pb.get('data_status'), pb.get('total_calls') or 0,
            pb.get('connected_calls') or 0, pb.get('connected_duration') or 0, last_shuffle,
            pb.get('shuffle_count') or 0, pb.get('lead_status'), lead_date, lead_time,
            pb.get('connected_employee'), pb.get('employee_name'), pb.get('employee_code'), (1 if pb.get('no_reallocation') else 0),
            pb_id, pb.get('created'), pb.get('updated') # Raw strings for comparison
        ))

    # 2. Build the Multi-row Insert Query
    placeholders = "(" + ", ".join(["%s"] * len(fields)) + ")"
    placeholders_list = ", ".join([placeholders] * len(values))
    
    # 3. Build the Update part (What to change if record already exists)
    # We exclude 'name', 'creation', 'owner', 'pb_id', 'pb_created' from update
    update_parts = [
        f"`{f}`=VALUES(`{f}`)" for f in fields if f not in ['name', 'creation', 'owner', 'pb_id', 'pb_created']
    ]
    
    query = f"""
        INSERT INTO `tabActive Database` ({', '.join(['`' + f + '`' for f in fields])}) 
        VALUES {placeholders_list}
        ON DUPLICATE KEY UPDATE {', '.join(update_parts)}
    """
    
    flattened_values = [val for row in values for val in row]
    frappe.db.sql(query, flattened_values)

def convert_dt(utc_str, both=False):
    """Robust and fast IST converter"""
    if not utc_str or str(utc_str).strip() == "":
        return (None, None) if both else None
    try:
        utc_str = str(utc_str).replace(' ', 'T')
        if utc_str.endswith('Z'): utc_str = utc_str[:-1]
        if '.' in utc_str:
            base, micros = utc_str.split('.')
            utc_str = f"{base}.{micros[:6]}"
        dt = datetime.fromisoformat(utc_str)
        ist = dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata'))
        return (ist.date(), ist.strftime("%H:%M:%S")) if both else ist.date()
    except Exception:
        return (None, None) if both else None

def fetch_pb_records(pb_url, pb_token, collection, page, per_page, pb_filter="", return_raw=False):
    try:
        url = f"{pb_url.rstrip('/')}/api/collections/{collection}/records"
        params = {"page": page, "perPage": per_page, "sort": "+updated", "filter": f"({pb_filter})" if pb_filter else ""}
        res = requests.get(url, headers={"Authorization": f"Bearer {pb_token}"}, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            return data if return_raw else data.get('items', [])
        return {} if return_raw else []
    except Exception:
        return {} if return_raw else []

def publish_progress(percentage, message, failed=False):
    if percentage > 100: percentage = 100
    frappe.publish_realtime("active_database_sync_progress", {"percentage": percentage, "message": message, "failed": failed})
