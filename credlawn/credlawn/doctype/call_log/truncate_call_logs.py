import frappe
from frappe import _

@frappe.whitelist()
def truncate_call_logs(password):
    """Deletes all local Call Log records with password verification."""
    # Verify current user's password for safety
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Truncate action cancelled."))

    # Direct SQL to clear the table
    frappe.db.sql("DELETE FROM `tabCall Log`")
    frappe.db.commit()
    return _("All Call Log Records have been cleared.")
