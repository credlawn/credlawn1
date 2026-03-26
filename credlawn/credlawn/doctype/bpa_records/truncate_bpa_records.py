import frappe
from frappe import _

@frappe.whitelist()
def truncate_bpa_records(password):
    """Deletes all records from BPA Records with password verification for security."""
    current_user = frappe.session.user
    
    # Verify the user's password before proceeding with the destructive action
    from frappe.utils.password import check_password
    try:
        check_password(current_user, password)
    except frappe.AuthenticationError:
        return _("Incorrect Password. Action Aborted.")

    # Execute the truncation
    frappe.db.sql("DELETE FROM `tabBPA Records`")
    frappe.db.commit()
    
    return _("BPA Records Truncated Successfully.")
