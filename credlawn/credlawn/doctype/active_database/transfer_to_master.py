import frappe
from frappe import _
from frappe.utils import now_datetime

@frappe.whitelist()
def execute_transfer():
    """Main entry for Transfer to Master - Enqueues background job"""
    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.active_database.transfer_to_master.transfer_job',
            queue='long',
            timeout=7200,
            is_async=True,
            job_name='transfer_to_master_job'
        )
        return _("Transfer to Master Database started in the background.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Transfer to Master Enqueue Error")
        return _("Failed to start transfer: {0}").format(str(e))

def transfer_job():
    """
    Batch transfers records from Active Database to Master Database.
    Conditions: transfer_to_master=1 and deleted_from_pb=1.
    If mobile_no exists in Master, it updates the record; otherwise, it creates a new one.
    """
    try:
        # 1. Fetch records ready for transfer
        records = frappe.get_all(
            "Active Database",
            filters={
                "transfer_to_master": 1,
                "deleted_from_pb": 1
            },
            fields=["*"]
        )

        if not records:
            publish_progress(100, "No records found matching transfer criteria.")
            return

        total = len(records)
        processed = 0
        created_count = 0
        updated_count = 0
        failed_count = 0
        
        # Batch processing for efficiency and safe rollback points
        batch_size = 100
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            mobiles = [r.mobile_no for r in batch if r.mobile_no]
            
            # Lookup existing records in Master by mobile_no to decide Update vs Insert
            existing_master = {}
            if mobiles:
                master_res = frappe.db.sql("""
                    SELECT name, mobile_no FROM `tabMaster Database` 
                    WHERE mobile_no IN %s
                """, (tuple(mobiles),), as_dict=True)
                existing_master = {r.mobile_no: r.name for r in master_res}

            success_ids = []
            
            for row in batch:
                try:
                    mobile = row.get('mobile_no')
                    
                    # Prepare data (exclude primary and metadata fields of the source)
                    doc_data = row.copy()
                    # Pop Frappe standard fields and the transfer flag itself
                    for key in [
                        'name', 'creation', 'modified', 'owner', 'modified_by', 
                        'transfer_to_master', 'doctype', '_user_tags', '_comments', 
                        '_assign', '_liked_by'
                    ]:
                        doc_data.pop(key, None)
                    
                    doc_data['doctype'] = 'Master Database'
                    
                    if mobile and mobile in existing_master:
                        # Update Existing Record in Master
                        master_doc = frappe.get_doc('Master Database', existing_master[mobile])
                        master_doc.update(doc_data)
                        master_doc.save(ignore_permissions=True)
                        updated_count += 1
                    else:
                        # Create New Record in Master
                        new_doc = frappe.get_doc(doc_data)
                        new_doc.insert(ignore_permissions=True)
                        created_count += 1
                    
                    # If we reached here, the Master operation succeeded
                    success_ids.append(row.name)
                    
                except Exception as e:
                    failed_count += 1
                    frappe.log_error(f"Transfer Error for {row.name}: {str(e)}", "Active Database Transfer Fail")

            # Atomic Step: Delete from Active ONLY if Master operation was successful
            if success_ids:
                frappe.db.sql("DELETE FROM `tabActive Database` WHERE name IN %s", (tuple(success_ids),))
                frappe.db.commit() # Save progress after every batch
            
            processed += len(batch)
            publish_progress(int((processed / total) * 100), f"Transferred {processed} of {total}...")

        summary = f"Transfer Complete! Created: {created_count}, Updated: {updated_count}, Failed: {failed_count}"
        publish_progress(100, summary)
        frappe.log_error(summary, "Active Database Transfer Summary")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Transfer to Master Job Fatal Error")
        publish_progress(0, "Transfer Failed. Check Error Log.", failed=True)

def publish_progress(percentage, message, failed=False):
    if percentage > 100: percentage = 100
    frappe.publish_realtime("active_database_transfer_progress", {"percentage": percentage, "message": message, "failed": failed})
