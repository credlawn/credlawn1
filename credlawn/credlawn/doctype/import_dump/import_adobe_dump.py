import frappe
import datetime
from frappe import _
from frappe.utils.xlsxutils import read_xlsx_file_from_attached_file
from frappe.utils import getdate

# Configuration: Maps Adobe Dump Fieldnames to their respective Excel Header Labels as defined by the user
FIELD_MAPPING = {
    "arn_no": "APPLICATION_REFERENCE_NUMBER",
    "dsa_code": "VARIABLE_VALUE",
    "arn_date": "CREATION_DATE_TIME",
    "lc1_code": "LC1_CODE",
    "lc2_code": "LC2_CODE",
    "sm_code": "SM_CODE",
    "customer_name": "CUSTOMER_NAME",
    "customer_type": "CUSTOMER_TYPE",
    "ipa_status": "IPA_STATUS",
    "promo_code": "PROMO_CODE",
    "state": "STATE",
    "product_code": "PRODUCT_CODE",
    "product_description": "Product Des",
    "company_name": "COMPANY_NAME",
    "pin_code": "PIN_CODE",
    "dap_final_flag": "DAP_FINAL_FLAG",
    "drop_off_reason": "DROPOFF_REASON",
    "vkyc_expiry_date": "VKYC_EXPIRY_DATE",
    "current_stage": "CURRENT_STAGE",
    "idcom_status": "IDCOM_STATUS",
    "vkyc_link": "CAPTURE_LINK",
    "kyc_status": "KYC Status",
    "vkyc_status": "VKYC_STATUS",
    "final_decision": "FINAL_DECISION",
    "final_decision_date": "FINAL_DECISION_DATE",
    "dap_final_date": "DAP_FINAL_DATE",
    "kyc_completion_date": "KYC_COMPLETION_DATE",
    "bkyc_status": "BKYC Status",
    "bkyc_reason": "Reason",
    "kyc_type": "VKYC_CONSENT_DATE",
    "decline_code": "DECLINE_CODE",
    "decline_type": "DECLINE_DESCRIPTION",
    "decline_description": "Decline Type",
    "decline_category": "Decline Descreption",
    "sm_remarks": "SM REMARK",
    "emil_curing": "EMAIL CURING",
    "card_type": "Card Type",
    "card_activation_status": "Card Activation Staus"
}

@frappe.whitelist()
def validate_import_file():
    """Performs a pre-import check: counts rows and validates existence of mapped headers."""
    import_doc = frappe.get_single("Import Dump")
    if not import_doc.attach_dump:
        frappe.throw(_("Please attach an Excel file first."))
        
    rows = read_xlsx_file_from_attached_file(file_url=import_doc.attach_dump)
    if not rows or len(rows) < 1:
        frappe.throw(_("The Excel file appears to be empty."))
        
    # Find the header row by searching for the unique ARN No identifier
    header_row_idx = -1
    search_term = str(FIELD_MAPPING["arn_no"]).strip().lower()
    for i, row in enumerate(rows[:20]):
        clean_row = [str(cell).strip().lower() if cell is not None else "" for cell in row]
        if search_term in clean_row:
            header_row_idx = i
            break
    
    if header_row_idx == -1:
        frappe.throw(_("Required header '{0}' not found in the first 20 rows.").format(FIELD_MAPPING["arn_no"]))
        
    headers = [str(h).strip() if h else "" for h in rows[header_row_idx]]
    all_data_rows = rows[header_row_idx + 1:]
    
    # Filter out empty rows to get an accurate count
    data_rows = [r for r in all_data_rows if any(c is not None and str(c).strip() != "" for c in r)]
    
    # Identify which mapped fields are missing from the current Excel file
    missing_headers = [header for header in FIELD_MAPPING.values() if header not in headers]
            
    return {
        "total_rows": len(data_rows),
        "missing_headers": missing_headers
    }

@frappe.whitelist()
def run_import_sample():
    """Enqueues the optimized background import process."""
    frappe.enqueue(
        "credlawn.credlawn.doctype.import_dump.import_adobe_dump.execute_import",
        queue="long",
        timeout=3600
    )
    return {"status": "success"}

def execute_import():
    """Optimized background import with real-time feedback and data integrity checks."""
    try:
        import_doc = frappe.get_single("Import Dump")
        dump_till = getdate(import_doc.dump_till)
        
        rows = read_xlsx_file_from_attached_file(file_url=import_doc.attach_dump)
        if not rows: return
            
        # Standard header identification
        header_row_idx = -1
        search_term = str(FIELD_MAPPING["arn_no"]).strip().lower()
        for i, row in enumerate(rows[:20]):
            if search_term in [str(cell).strip().lower() if cell is not None else "" for cell in row]:
                header_row_idx = i
                break
        
        headers = [str(h).strip() if h else "" for h in rows[header_row_idx]]
        all_data_rows = rows[header_row_idx + 1:]
        data_rows = [r for r in all_data_rows if any(c is not None and str(c).strip() != "" for c in r)]
        
        excel_to_field = {v: k for k, v in FIELD_MAPPING.items()}
        total_rows = len(data_rows)
        
        # Pre-cache existing records to handle updates efficiently
        existing_records = {
            d.arn_no.strip().upper(): {"name": d.name, "date": d.adobe_dump_date}
            for d in frappe.get_all("Adobe Dump", fields=["name", "arn_no", "adobe_dump_date"]) 
            if d.arn_no
        }
        
        counters = {"created": 0, "updated": 0, "skipped": 0, "processed": 0}
        
        for row_idx, row in enumerate(data_rows):
            counters["processed"] += 1
            
            doc_data = {"doctype": "Adobe Dump", "adobe_dump_date": dump_till}
            
            # Map Excel columns to Doctype fields
            for i, label in enumerate(headers):
                if label in excel_to_field:
                    fname = excel_to_field[label]
                    val = row[i] if i < len(row) else None
                    if val is not None and str(val).strip().upper() == "#N/A": val = None
                    
                    # Core Date Parsing (Handles MM/DD/YYYY and Excel Serials)
                    if val:
                        if fname in ["arn_date", "final_decision_date", "dap_final_date", "kyc_completion_date", "kyc_type"]:
                            try: val = getdate(val)
                            except: pass
                        elif fname == "vkyc_expiry_date":
                            try:
                                # Prioritize numeric serial conversion for datetimes
                                serial_val = float(val)
                                val = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=serial_val)
                            except:
                                try: val = getdate(val)
                                except: pass
                    
                    doc_data[fname] = val
            
            # Application Reference Number (ARN) check
            arn_no = str(doc_data.get("arn_no", "")).strip().upper()
            if not arn_no: continue
            doc_data["arn_no"] = arn_no
            
            try:
                if arn_no in existing_records:
                    # Date Constraint: Only update if current data is older or equal
                    existing = existing_records[arn_no]
                    if existing["date"] and dump_till and getdate(existing["date"]) > dump_till:
                        counters["skipped"] += 1
                        continue

                    doc = frappe.get_doc("Adobe Dump", existing["name"])
                    doc.update(doc_data)
                    doc.save(ignore_permissions=True)
                    counters["updated"] += 1
                else:
                    doc = frappe.get_doc(doc_data)
                    doc.insert(ignore_permissions=True)
                    existing_records[arn_no] = {"name": doc.name, "date": dump_till}
                    counters["created"] += 1
            except Exception as e:
                frappe.log_error(f"Row {row_idx + 2} Error: {str(e)}", "Adobe Dump Import")
            
            # Periodic Progress & Commit
            if counters["processed"] % 50 == 0 or counters["processed"] == total_rows:
                percentage = int((counters["processed"] / total_rows) * 100)
                msg = _("Processing {0} of {1}...").format(counters["processed"], total_rows)
                publish_progress(percentage, msg)
                
            if counters["processed"] % 200 == 0:
                frappe.db.commit()

        frappe.db.commit()
        summary = _("Import Finished! Created: {0}, Updated: {1}, Skipped: {2}").format(
            counters["created"], counters["updated"], counters["skipped"]
        )
        publish_progress(100, summary)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Adobe Dump Import Critical Failure")
        publish_progress(0, _("Import failed: {0}").format(str(e)), failed=True)

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("adobe_import_progress", {"percentage": percentage, "message": message, "failed": failed})

@frappe.whitelist()
def clear_import_fields():
    """Resets the Import Dump Single Doctype fields and deletes the attachment."""
    import_doc = frappe.get_single("Import Dump")
    file_url = import_doc.attach_dump
    
    if file_url:
        # Delete associated File records to clean up storage
        files = frappe.get_all("File", filters={"file_url": file_url})
        for f in files:
            frappe.delete_doc("File", f.name, ignore_permissions=True)
            
    import_doc.attach_dump = None
    import_doc.dump_till = None
    import_doc.dump_type = None
    import_doc.save(ignore_permissions=True)
    
    return {"status": "success", "message": _("Form cleared and file deleted.")}
