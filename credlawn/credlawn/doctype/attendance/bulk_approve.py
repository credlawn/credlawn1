import frappe
from frappe import _
from frappe.utils import get_time

@frappe.whitelist()
def execute_approval(payroll_month):
    """Bulk updates approved_status based on punch timings for a specific payroll month."""
    if not payroll_month:
        return _("Please specify a Payroll Month.")

    # 1. Selection Criteria:
    # - Specific Payroll Month
    # - Approval Type is NOT Manager
    # - Skip special statuses that shouldn't be auto-overwritten (Holiday, Leave)
    # - We primarily process Present, Pending, Half-day, Full-day records
    records = frappe.get_all("Attendance", 
        filters={
            "payroll_month": payroll_month,
            "approval_type": ["!=", "Manager"],
            "approved_status": ["not in", ["Holiday", "Leave", "Absent"]]
        },
        fields=["name"]
    )

    if not records:
        return _("No eligible records found for {0}.").format(payroll_month)

    # 2. Timing Thresholds (Strict)
    # Full day if: In before 10:16 AM AND Out after 06:30 PM
    limit_in = get_time("10:16:00")
    limit_out = get_time("18:30:00")

    count_full = 0
    count_half = 0
    
    for r in records:
        try:
            doc = frappe.get_doc("Attendance", r.name)
            
            check_in = get_time(doc.check_in_time) if doc.check_in_time else None
            check_out = get_time(doc.check_out_time) if doc.check_out_time else None
            
            # Logic Check
            is_valid_in = check_in and check_in < limit_in
            is_valid_out = check_out and check_out >= limit_out
            
            if is_valid_in and is_valid_out:
                doc.approved_status = "Full-day"
                count_full += 1
            else:
                # Missing punches or late/early departure result in Half-day
                doc.approved_status = "Half-day"
                count_half += 1
            
            # Save will trigger doc.validate() -> Recalculates salary amounts
            doc.save(ignore_permissions=True)
            
        except Exception as e:
            frappe.log_error(f"Bulk Approval Error for {r.name}: {str(e)}", "Bulk Approval Error")

    frappe.db.commit()
    
    summary = _("Approval Processing Complete for {0}:").format(payroll_month)
    summary += f"\n- {count_full} records set to Full-day"
    summary += f"\n- {count_half} records set to Half-day"
    
    return summary
