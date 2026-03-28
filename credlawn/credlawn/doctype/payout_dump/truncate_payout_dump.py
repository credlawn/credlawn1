import frappe
from frappe import _
from frappe.utils.password import check_password

@frappe.whitelist()
def truncate_payout_dump(password):
    """Securely deletes all records from the Payout Dump doctype after password verification."""
    # 1. Security Check: Verify user's identity
    user = frappe.session.user
    if not user:
        frappe.throw(_("Session expired. Please log in again."))
        
    try:
        check_password(user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Truncate operation aborted."), frappe.AuthenticationError)

    # 2. Performance: Direct SQL for fast truncation of large datasets
    # Using frappe.db.sql for speed while maintaining metadata integrity
    frappe.db.sql("DELETE FROM `tabPayout Dump`")
    frappe.db.commit()
    
    return _("Success: All Payout Dump records have been securely deleted.")
