import frappe
from frappe import _

@frappe.whitelist()
def execute_truncate(password):
    """Deletes all local Active Database records with password verification."""
    from frappe.auth import LoginManager
    login_manager = LoginManager()
    
    try:
        login_manager.check_password(frappe.session.user, password)
    except frappe.AuthenticationError:
        frappe.throw(_("Invalid password. Truncate action cancelled."))

    # Direct SQL to clear the table
    frappe.db.sql("DELETE FROM `tabActive Database`")
    frappe.db.commit()
    return _("All local Active Database records have been cleared.")
