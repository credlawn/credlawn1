import frappe
from frappe import _
from credlawn.credlawn.doctype.adobe_dump.adobe_dump import sync_and_enrich_data as sync_adobe
from credlawn.credlawn.doctype.dsa_dump.dsa_dump import sync_dsa_data as sync_dsa

@frappe.whitelist()
def remap_all_data():
    """
    Unified manual reconciliation for both Adobe and DSA Dumps.
    Scans unvalidated IPA Records and triggers sync from both sources.
    """
    # 1. Fetch IPA Records that might need re-mapping
    # We check those not validated in either Adobe or DSA
    ipa_records = frappe.get_all(
        "IPA Records",
        filters={
            "arn_no": ["is", "set"]
        },
        fields=["name", "arn_no", "adobe_dump_validated", "dsa_dump_validated"]
    )
    
    if not ipa_records:
        return _("No IPA Records found for re-mapping.")
        
    adobe_matches = 0
    dsa_matches = 0
    total_checked = len(ipa_records)
    
    for ipa in ipa_records:
        arn_no = str(ipa.arn_no).strip().upper()
        
        # A. Check Adobe Dump
        if ipa.adobe_dump_validated != "Yes":
            adobe_doc_name = frappe.db.get_value("Adobe Dump", {"arn_no": arn_no})
            if adobe_doc_name:
                try:
                    adobe_doc = frappe.get_doc("Adobe Dump", adobe_doc_name)
                    sync_adobe(adobe_doc)
                    adobe_matches += 1
                except:
                    pass

        # B. Check DSA Dump
        if ipa.dsa_dump_validated != "Yes":
            dsa_doc_name = frappe.db.get_value("DSA Dump", {"arn_no": arn_no})
            if dsa_doc_name:
                try:
                    dsa_doc = frappe.get_doc("DSA Dump", dsa_doc_name)
                    sync_dsa(dsa_doc)
                    dsa_matches += 1
                except:
                    pass
                    
    frappe.db.commit()
    
    summary = _("Re-map Complete! Checked {0} records. Mapped {1} Adobe and {2} DSA matches.") \
              .format(total_checked, adobe_matches, dsa_matches)
              
    return summary
