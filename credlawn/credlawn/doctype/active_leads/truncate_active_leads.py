import frappe
from frappe import _

@frappe.whitelist()
def truncate_active_leads(password):
    """Deletes all local Active Leads records with password verification for safety."""
    # Verify current user's password
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Truncate action cancelled."))

    # Direct SQL to clear the table
    frappe.db.sql("DELETE FROM `tabActive Leads`")
    frappe.db.commit()
    return _("All local Active Lead records have been cleared.")
