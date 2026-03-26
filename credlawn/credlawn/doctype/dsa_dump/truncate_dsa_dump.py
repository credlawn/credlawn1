import frappe
from frappe import _
from frappe.utils.password import check_password

@frappe.whitelist()
def truncate_dsa_dump(password):
    """Securely deletes all records from the DSA Dump doctype after password verification."""
    # 1. Security Check: Verify user's identity
    user = frappe.session.user
    try:
        check_password(user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Truncate operation aborted."), frappe.AuthenticationError)

    # 2. Performance: Direct SQL for fast truncation of large datasets
    # Using frappe.db.sql for speed while maintaining metadata integrity
    frappe.db.sql("DELETE FROM `tabDSA Dump`")
    frappe.db.commit()
    
    # 3. Cleanup: Clear any cached mapping data if necessary
    frappe.cache().delete_value("dsa_dump_records") # Optional internal cache
    
    return _("Success: All DSA Dump records have been securely deleted.")
