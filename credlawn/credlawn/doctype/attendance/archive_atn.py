import frappe
from frappe import _
import requests
import json

@frappe.whitelist()
def execute_archive(password):
    """Main entry point to start the background archive job."""
    # Verify current user's password for safety
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Archive action cancelled."))

    # Start background job
    frappe.enqueue(
        "credlawn.credlawn.doctype.attendance.archive_atn.archive_job",
        queue="long",
        timeout=3600,
        job_name="attendance_archive_pb"
    )
    
    return _("Archive process started in the background. You will be notified when complete.")

def archive_job():
    """Background job to delete records from Pocketbase in batches."""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            frappe.log_error("Pocketbase configuration missing", "Archive Job Error")
            return

        # Fetch records to archive
        records_to_archive = frappe.get_all(
            "Attendance", 
            filters={"clean_from_pb": 1, "pb_id": ["is", "set"]}, 
            fields=["name", "pb_id"]
        )

        if not records_to_archive:
            publish_progress(100, "No records found to archive.")
            return

        total_records = len(records_to_archive)
        batch_size = 50 # Pocketbase recommended batch size for safety
        processed_count = 0
        success_count = 0
        failed_count = 0

        api_url = f"{pb_url.rstrip('/')}/api/batch"
        headers = {
            "Authorization": f"Bearer {pb_token}",
            "Content-Type": "application/json"
        }

        for i in range(0, total_records, batch_size):
            batch = records_to_archive[i:i + batch_size]
            
            # Construct batch delete request
            batch_requests = []
            for record in batch:
                batch_requests.append({
                    "method": "DELETE",
                    "url": f"/api/collections/attendance/records/{record.pb_id}"
                })
            
            try:
                response = requests.post(
                    api_url, 
                    headers=headers, 
                    json={"requests": batch_requests}, 
                    timeout=60
                )
                
                if response.status_code == 200:
                    # Successfully processed batch
                    success_count += len(batch)

                    # Bulk update Frappe records using direct SQL for optimization
                    batch_names = [record.name for record in batch]
                    frappe.db.sql("""
                        UPDATE `tabAttendance` 
                        SET 
                            clean_from_pb = 0,
                            deleted_from_pb = 1,
                            pb_id = '', 
                            check_in_selfie = '', 
                            check_out_selfie = '' 
                        WHERE name IN %s
                    """, (tuple(batch_names),))
                else:
                    failed_count += len(batch)
                    frappe.log_error(f"PB Batch Delete Error: {response.text}", "Archive Job Error")
                
            except Exception as e:
                failed_count += len(batch)
                frappe.log_error(f"Batch Request Failed: {str(e)}", "Archive Job Error")

            processed_count += len(batch)
            progress = int((processed_count / total_records) * 100)
            publish_progress(progress, f"Archived {processed_count} of {total_records} records...")
            
            frappe.db.commit()

        summary = f"Archive Completed! Success: {success_count}, Failed: {failed_count}"
        frappe.log_error(summary, "Archive Job Summary")
        publish_progress(100, summary)

    except Exception as e:
        frappe.db.rollback()
        frappe.log_error(frappe.get_traceback(), "Archive Job Fatal Error")
        publish_progress(0, "Archiving failed. Check Error Logs.", failed=True)

def publish_progress(percentage, message, failed=False):
    """Publish real-time progress via Socket.io."""
    frappe.publish_realtime("attendance_archive_progress", {
        "percentage": percentage,
        "message": message,
        "failed": failed
    })
