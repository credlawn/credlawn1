import frappe
from frappe import _
import requests
from datetime import datetime
import pytz

@frappe.whitelist()
def execute_sync():
    """Main entry point for Attendance Sync - Enqueues background job"""
    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.attendance.sync_atn.sync_job',
            queue='long',
            timeout=7200,
            is_async=True,
            job_name='attendance_sync'
        )
        return _("Attendance Sync started in the background. You will be notified when complete.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), _("Attendance Sync Enqueue Error"))
        return _("Failed to start Attendance Sync: {0}").format(str(e))

def sync_job():
    """Background job worker with optimized comparison logic"""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            frappe.log_error("Pocketbase credentials missing", "Attendance Sync Job Error")
            return

        # Step 1: Pre-fetch pb_id and pb_updated for O(1) lookup and comparison
        existing_records = {
            d.pb_id: d.pb_updated for d in frappe.get_all("Attendance", 
            filters={"pb_id": ["is", "set"]}, 
            fields=["pb_id", "pb_updated"])
        }

        total_fetched = 0
        created_count = 0
        updated_count = 0
        skipped_count = 0
        failed_count = 0

        page = 1
        per_page = 200
        
        # Initial call to get totalItems
        initial_res = fetch_pocketbase_records(pb_url, pb_token, page=1, per_page=1, return_raw=True)
        total_items = initial_res.get('totalItems', 0) if initial_res else 0

        while True:
            records = fetch_pocketbase_records(pb_url, pb_token, page=page, per_page=per_page)
            if not records:
                break

            for record in records:
                try:
                    pb_id = record.get('id')
                    pb_updated = record.get('updated') # Raw ISO string from PB
                    
                    if not pb_id:
                        continue

                    # Step 2: Compare PB updated timestamp with local one
                    if pb_id in existing_records:
                        if existing_records[pb_id] == pb_updated:
                            skipped_count += 1
                            continue # Skip update if timestamps match
                        
                        # Timestamps differ, update record
                        doc_name = frappe.db.get_value("Attendance", {"pb_id": pb_id}, "name")
                        doc = frappe.get_doc('Attendance', doc_name)
                        doc.flags.from_pb_sync = True
                        update_doc_fields(doc, record)
                        doc.save(ignore_permissions=True)
                        updated_count += 1
                    else:
                        # New record
                        doc = frappe.new_doc('Attendance')
                        doc.flags.from_pb_sync = True
                        update_doc_fields(doc, record)
                        doc.insert(ignore_permissions=True)
                        created_count += 1
                        # Update local dict to prevent duplicates in same batch
                        existing_records[pb_id] = pb_updated
                        
                except Exception as e:
                    failed_count += 1
                    frappe.log_error(f"Failed to sync record {record.get('id')}: {str(e)}", "Attendance Sync Record Error")

            total_fetched += len(records)
            if total_items > 0:
                progress = int((total_fetched / total_items) * 100)
                publish_progress(progress, f"Syncing {total_fetched} of {total_items} records...")

            if len(records) < per_page:
                break
            page += 1
            frappe.db.commit()

        frappe.db.commit()
        summary = f"Attendance Sync Completed! Total: {total_fetched}, Created: {created_count}, Updated: {updated_count}, Skipped: {skipped_count}, Failed: {failed_count}"
        frappe.log_error(summary, "Attendance Sync Completed")
        publish_progress(100, summary)

    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), "Attendance Sync Job Fatal Error")
        publish_progress(0, "Sync failed. Check Error Logs.", failed=True)

def fetch_pocketbase_records(pb_url, pb_token, page=1, per_page=500, return_raw=False):
    try:
        api_url = f"{pb_url.rstrip('/')}/api/collections/attendance/records"
        headers = {"Authorization": f"Bearer {pb_token}", "Content-Type": "application/json"}
        params = {"perPage": per_page, "page": page}
        response = requests.get(api_url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return data if return_raw else data.get('items', [])
        return {} if return_raw else []
    except Exception:
        return {} if return_raw else []

def update_doc_fields(doc, pb_record):
    """Map Pocketbase fields to Frappe Doctype fields"""
    # 1. Status mapping
    status_val = pb_record.get('status')
    doc.approved_status = status_val if status_val else "Pending"
    
    # 2. Date and Time splitting (UTC -> IST)
    check_in_utc = pb_record.get('check_in_time')
    if check_in_utc:
        ist_dt = convert_to_ist(check_in_utc)
        if ist_dt:
            doc.attendance_date = ist_dt.date()
            doc.check_in_time = ist_dt.strftime("%H:%M:%S")
            
    check_out_utc = pb_record.get('check_out_time')
    if check_out_utc:
        ist_dt = convert_to_ist(check_out_utc)
        if ist_dt:
            doc.check_out_time = ist_dt.strftime("%H:%M:%S")

    # 3. Direct mappings
    doc.employee_name = pb_record.get('employee_name')
    doc.employee_code = pb_record.get('employee_code')
    doc.check_in_latitude = pb_record.get('check_in_latitude')
    doc.check_in_longitude = pb_record.get('check_in_longitude')
    doc.check_out_latitude = pb_record.get('check_out_latitude')
    doc.check_out_longitude = pb_record.get('check_out_longitude')
    doc.address = pb_record.get('address')
    doc.pb_id = pb_record.get('id')
    doc.clean_from_pb = 1 if pb_record.get('remove_data') else 0
    
    # 4. Selfie URLs construction
    collection_id = pb_record.get('collectionId') or "pbc_2471705857"
    record_id = pb_record.get('id')
    pb_url = frappe.conf.get("pocketbase_url").rstrip('/')

    if pb_record.get('check_in_selfie'):
        doc.check_in_selfie = f"{pb_url}/api/files/{collection_id}/{record_id}/{pb_record.get('check_in_selfie')}"
    if pb_record.get('check_out_selfie'):
        doc.check_out_selfie = f"{pb_url}/api/files/{collection_id}/{record_id}/{pb_record.get('check_out_selfie')}"

    # 5. Store RAW PB Timestamps for O(1) comparison in future syncs
    doc.pb_created = pb_record.get('created')
    doc.pb_updated = pb_record.get('updated')
        
    return True

def push_status_to_pb(doc_name):
    """Pushes the updated approved_status back to Pocketbase."""
    try:
        doc = frappe.get_doc('Attendance', doc_name)
        if not doc.pb_id:
            return

        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        api_url = f"{pb_url.rstrip('/')}/api/collections/attendance/records/{doc.pb_id}"
        headers = {
            "Authorization": f"Bearer {pb_token}",
            "Content-Type": "application/json"
        }
        
        # Pocketbase uses 'status' field for approval tracking
        data = {"status": doc.approved_status}
        
        response = requests.patch(api_url, headers=headers, json=data, timeout=30)
        
        if response.status_code != 200:
            frappe.log_error(f"Failed to push status to PB: {response.text}", "Attendance Status Sync Error")
            
    except Exception:
        frappe.log_error(frappe.get_traceback(), "Attendance Status Sync Job Error")

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("attendance_sync_progress", {"percentage": percentage, "message": message, "failed": failed})

def convert_to_ist(utc_datetime_str):
    if not utc_datetime_str: return None
    try:
        utc_datetime_str = str(utc_datetime_str).replace(' ', 'T')
        if utc_datetime_str.endswith('Z'): utc_datetime_str = utc_datetime_str[:-1]
        utc_dt = datetime.fromisoformat(utc_datetime_str)
        utc_tz = pytz.UTC
        if utc_dt.tzinfo is None: utc_dt = utc_tz.localize(utc_dt)
        ist_tz = pytz.timezone('Asia/Kolkata')
        ist_dt = utc_dt.astimezone(ist_tz)
        return ist_dt.replace(tzinfo=None)
    except Exception: return None
