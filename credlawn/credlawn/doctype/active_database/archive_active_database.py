import frappe
from frappe import _
import requests

@frappe.whitelist()
def execute_archive(password):
    """Main entry point for Active Database archiving. Requires password for safety."""
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Archive cancelled."))

    frappe.enqueue(
        "credlawn.credlawn.doctype.active_database.archive_active_database.archive_job",
        queue="long",
        timeout=3600,
        job_name="active_database_archive_pb"
    )
    
    return _("Active Database archiving started in the background.")

def archive_job():
    """Silent background job to batch delete marked records from PocketBase."""
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        # Fetch records marked to archive
        records_to_archive = frappe.get_all(
            "Active Database", 
            filters={"clean_from_pb": 1, "pb_id": ["is", "set"]}, 
            fields=["name", "pb_id"]
        )

        if not records_to_archive:
            return

        total_records = len(records_to_archive)
        batch_size = 50
        success_count = 0
        
        # PocketBase Batch API URL
        api_url = f"{pb_url.rstrip('/')}/api/batch"
        headers = {"Authorization": f"Bearer {pb_token}", "Content-Type": "application/json"}
        collection_id = "pbc_2300403255"

        for i in range(0, total_records, batch_size):
            batch = records_to_archive[i:i + batch_size]
            batch_requests = [{"method": "DELETE", "url": f"/api/collections/{collection_id}/records/{r.pb_id}"} for r in batch]
            
            try:
                response = requests.post(api_url, headers=headers, json={"requests": batch_requests}, timeout=60)
                
                if response.status_code == 200:
                    success_count += len(batch)
                    
                    # Update local records to indicate they are no longer in PocketBase
                    batch_names = [r.name for r in batch]
                    frappe.db.sql("""
                        UPDATE `tabActive Database` 
                        SET 
                            clean_from_pb = 0, 
                            deleted_from_pb = 1, 
                            pb_id = '' 
                        WHERE name IN %s
                    """, (tuple(batch_names),))
                else:
                    frappe.log_error(f"Active Database PB Archive Error: {response.text}", "Active Database Archive")

            except Exception as e:
                frappe.log_error(f"Batch Request Failed: {str(e)}", "Active Database Archive Error")

            frappe.db.commit()

        frappe.log_error(f"Successfully archived {success_count} records from PocketBase", "Active Database Archive Complete")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Active Database Archive Job Fatal Error")
