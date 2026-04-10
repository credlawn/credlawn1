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
        return _("High-Speed Active Database Sync started in the background.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Database Sync Enqueue Error")
        return _("Failed to start sync: {0}").format(str(e))

def sync_job():
    """Optimized background job using Timestamp Checkpoint and Batch SQL Insert/Update"""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            frappe.log_error("PocketBase credentials missing in site_config", "Active Database Sync Error")
            return

        # 1. Checkpoint - Find the latest pb_updated in Frappe to only fetch new/modified data
        latest_pb_updated = frappe.db.get_value("Active Database", 
            filters={"pb_id": ["is", "set"]}, 
            fieldname="pb_updated", 
            order_by="pb_updated desc"
        )

        pb_filter = ""
        if latest_pb_updated:
            pb_filter = f"updated > '{latest_pb_updated}'"

        page = 1
        per_page = 500
        # Using Collection ID directly as provided in your JSON to be safe
        collection_name = "pbc_2300403255" 
        
        total_fetched = 0
        inserted_count = 0
        updated_count = 0
        failed_count = 0
        
        # Get total count for progress estimation
        initial_res = fetch_pb_records(pb_url, pb_token, collection_name, 1, 1, pb_filter, return_raw=True)
        
        # Connectivity Debug
        if not initial_res or 'totalItems' not in initial_res:
             frappe.log_error(f"PB Response Error: {initial_res}", "Active Database Sync Debug")
        
        total_items = initial_res.get('totalItems', 0) if initial_res else 0

        while True:
            records = fetch_pb_records(pb_url, pb_token, collection_name, page, per_page, pb_filter)
            if not records:
                break

            fetched_ids = [r.get('id') for r in records if r.get('id')]
            if not fetched_ids:
                break
            
            # Map existing records to decide between Insert or Update
            existing_db = frappe.db.sql(
                "SELECT pb_id, name FROM `tabActive Database` WHERE pb_id IN %s", 
                (tuple(fetched_ids),), as_dict=True
            )
            existing_map = {r.pb_id: r.name for r in existing_db}

            to_insert = []
            to_update = []

            for record in records:
                pb_id = record.get('id')
                if not pb_id: continue

                if pb_id in existing_map:
                    record['frappe_name'] = existing_map[pb_id]
                    to_update.append(record)
                else:
                    record['frappe_name'] = pb_id # Using PB ID as the document name
                    to_insert.append(record)

            try:
                if to_insert:
                    perform_batch_insert(to_insert)
                    inserted_count += len(to_insert)
                if to_update:
                    perform_batch_update(to_update)
                    updated_count += len(to_update)
            except Exception as e:
                frappe.log_error(f"Batch SQL Error: {str(e)}\n\n{frappe.get_traceback()}", "Active Database Sync Error")
                failed_count += len(records)

            total_fetched += len(records)
            
            # Real-time progress update
            if total_items > 0:
                progress = int((total_fetched / total_items) * 100)
                if progress > 100: progress = 100
                publish_progress(progress, f"Syncing: {total_fetched} of {total_items}...")

            if len(records) < per_page:
                break
            
            page += 1
            frappe.db.commit()

        frappe.db.commit()
        summary = f"Sync Complete! Inserted: {inserted_count}, Updated: {updated_count}, Failed: {failed_count}"
        publish_progress(100, summary if total_items > 0 else "Everything is up-to-date.")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Database Sync Fatal Error")
        publish_progress(0, "Sync Failed. Check Error Log.", failed=True)

def perform_batch_insert(records):
    """High-speed batch insertion for new records"""
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
        lead_date, lead_time = convert_dt(pb.get('lead_status_date'), both=True)
        old_dec_date = convert_dt(pb.get('old_decision_date'))
        import_date = convert_dt(pb.get('import_date'))
        last_shuffle = convert_dt(pb.get('last_shuffle_date'))
        
        values.append((
            pb.get('frappe_name'), now_val, now_val, 'Administrator', 'Administrator', 0,
            pb.get('customer_name'), pb.get('mobile_no'), pb.get('city'), pb.get('segment'), pb.get('employer'),
            pb.get('decline_reason'), pb.get('product'), pb.get('old_arn_no'), old_dec_date,
            pb.get('promo_code'), import_date, pb.get('data_code'), pb.get('data_sub_code'), pb.get('custom_code'),
            pb.get('allocation_count') or 0, pb.get('employee_count') or 0, pb.get('data_status'), pb.get('total_calls') or 0,
            pb.get('connected_calls') or 0, pb.get('connected_duration') or 0, last_shuffle,
            pb.get('shuffle_count') or 0, pb.get('lead_status'), lead_date, lead_time,
            pb.get('connected_employee'), pb.get('employee_name'), pb.get('employee_code'), (1 if pb.get('no_reallocation') else 0),
            pb.get('id'), pb.get('created'), pb.get('updated')
        ))

    placeholders = "(" + ", ".join(["%s"] * len(fields)) + ")"
    placeholders_list = ", ".join([placeholders] * len(values))
    flattened_values = [val for row in values for val in row]

    query = f"INSERT IGNORE INTO `tabActive Database` ({', '.join(['`' + f + '`' for f in fields])}) VALUES {placeholders_list}"
    frappe.db.sql(query, flattened_values)

def perform_batch_update(records):
    """High-speed batch updates for existing records to refresh counts and statuses"""
    now_val = now_datetime()
    for pb in records:
        lead_date, lead_time = convert_dt(pb.get('lead_status_date'), both=True)
        old_dec_date = convert_dt(pb.get('old_decision_date'))
        import_date = convert_dt(pb.get('import_date'))
        last_shuffle = convert_dt(pb.get('last_shuffle_date'))

        frappe.db.sql("""
            UPDATE `tabActive Database` SET
                modified=%s, customer_name=%s, mobile_no=%s, city=%s, segment=%s, employer=%s,
                decline_reason=%s, product=%s, old_arn_no=%s, old_decision_date=%s,
                promo_code=%s, import_date=%s, data_code=%s, data_sub_code=%s, custom_code=%s,
                allocation_count=%s, employee_count=%s, data_status=%s, total_calls=%s,
                connected_calls=%s, connected_duration=%s, last_shuffle_date=%s,
                shuffle_count=%s, lead_status=%s, lead_status_date=%s, lead_status_time=%s,
                connected_employee=%s, employee_name=%s, employee_code=%s, no_reallocation=%s,
                pb_updated=%s
            WHERE pb_id=%s
        """, (
            now_val, pb.get('customer_name'), pb.get('mobile_no'), pb.get('city'), pb.get('segment'), pb.get('employer'),
            pb.get('decline_reason'), pb.get('product'), pb.get('old_arn_no'), old_dec_date,
            pb.get('promo_code'), import_date, pb.get('data_code'), pb.get('data_sub_code'), pb.get('custom_code'),
            pb.get('allocation_count') or 0, pb.get('employee_count') or 0, pb.get('data_status'), pb.get('total_calls') or 0,
            pb.get('connected_calls') or 0, pb.get('connected_duration') or 0, last_shuffle,
            pb.get('shuffle_count') or 0, pb.get('lead_status'), lead_date, lead_time,
            pb.get('connected_employee'), pb.get('employee_name'), pb.get('employee_code'), (1 if pb.get('no_reallocation') else 0),
            pb.get('updated'), pb.get('id')
        ))

def convert_dt(utc_str, both=False):
    """Robust UTC to IST converter with null/blank safety"""
    if not utc_str or str(utc_str).strip() == "":
        return (None, None) if both else None
    try:
        # Standardize format (PocketBase often uses space or T)
        utc_str = str(utc_str).replace(' ', 'T')
        if utc_str.endswith('Z'): utc_str = utc_str[:-1]
        
        # Handle microsecond clipping beyond 6 digits for ISO format
        if '.' in utc_str:
            base, micros = utc_str.split('.')
            utc_str = f"{base}.{micros[:6]}"
            
        dt = datetime.fromisoformat(utc_str)
        ist = dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata'))
        
        if both:
            return (ist.date(), ist.strftime("%H:%M:%S"))
        return ist.date()
    except Exception:
        return (None, None) if both else None

def fetch_pb_records(pb_url, pb_token, collection, page, per_page, pb_filter="", return_raw=False):
    try:
        url = f"{pb_url.rstrip('/')}/api/collections/{collection}/records"
        headers = {"Authorization": f"Bearer {pb_token}"}
        params = {
            "page": page, 
            "perPage": per_page, 
            "sort": "+updated", # Process oldest changes first
            "filter": f"({pb_filter})" if pb_filter else ""
        }
        res = requests.get(url, headers=headers, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            return data if return_raw else data.get('items', [])
        return {} if return_raw else []
    except Exception:
        return {} if return_raw else []

def publish_progress(percentage, message, failed=False):
    if percentage > 100: percentage = 100
    frappe.publish_realtime("active_database_sync_progress", {"percentage": percentage, "message": message, "failed": failed})
