import frappe
from frappe import _

@frappe.whitelist()
def map_employee():
    """
    Looks for BPA Records with missing employee details and attempts to map them 
    from IPA Records using the ARN Number.
    """
    # 1. Identify BPA Records that need mapping
    bpa_to_fix = frappe.get_all(
        "BPA Records",
        filters={
            "employee_name": ["in", [None, "", "NULL"]]
        },
        fields=["name", "arn_no"]
    )
    
    if not bpa_to_fix:
        return _("Status: All BPA Records are already fully mapped.")
        
    mapped_count = 0
    total_checked = len(bpa_to_fix)
    
    # 2. Iterate and sync from IPA Records
    for record in bpa_to_fix:
        arn_no = str(record.arn_no).strip().upper() if record.arn_no else None
        if not arn_no:
            continue
            
        # Lookup matching IPA record
        ipa_match = frappe.db.get_value(
            "IPA Records",
            {"arn_no": arn_no},
            ["employee_name", "employee_code", "mobile_no", "name"],
            as_dict=True
        )
        
        if ipa_match:
            # Sync to BPA
            frappe.db.set_value("BPA Records", record.name, {
                "employee_name": ipa_match.employee_name,
                "employee_code": ipa_match.employee_code,
                "mobile_no": ipa_match.mobile_no
            }, update_modified=True)
            
            # Update IPA Verification status
            frappe.db.set_value("IPA Records", ipa_match.name, "final_approved_count_verified", "Yes")
            
            mapped_count += 1
            
    if mapped_count > 0:
        frappe.db.commit()
        return _("Mapped {0} of {1} missing records successfully.").format(mapped_count, total_checked)
    else:
        return _("Checked {0} records, but no matches found in IPA Records. Please ensure ARN numbers match exactly.").format(total_checked)
