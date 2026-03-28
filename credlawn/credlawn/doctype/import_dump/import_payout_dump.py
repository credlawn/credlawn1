import frappe
from frappe import _
from frappe.utils.xlsxutils import read_xlsx_file_from_attached_file
from frappe.utils import getdate

# Field mapping for Payout Dump (Phase 1)
FIELD_MAPPING = {
    "arn_no": "APPLNREF",
    "dsa_code": "TEAMCD",
    "sm_code": "SM",
    "lc1_code": "LC",
    "lc2_code": "LG",
    "decision_date": "DECISIN_DT",
    "product": "Des",
    "payout_computation": "Final Remarks Activation Status for Payout Computation",
    "final_amount_in_dump": "Final Rate",
    "city": "CITY",
    "state": "State",
    "employer": "EMPLOYER",
    "market": "Market",
    "carding_month": "Month",
    "card_classification": "Final Credit Card Classification",
    "activation_type": "Activation Type",
    "card_level_payout_100": "Card Level Payout 100%",
    "card_level_payout_80": "Card Level Payout 80%",
    "card_level_payout_20": "Card Level Payout 20%"
}

@frappe.whitelist()
def validate_import_file():
    import_doc = frappe.get_single("Import Dump")
    if not import_doc.attach_dump:
        frappe.throw(_("Please attach an Excel file first."))
        
    rows = read_xlsx_file_from_attached_file(file_url=import_doc.attach_dump)
    if not rows or len(rows) < 1:
        frappe.throw(_("The Excel file appears to be empty."))
        
    header_row_idx = -1
    search_term = str(FIELD_MAPPING["arn_no"]).strip().lower()
    for i, row in enumerate(rows[:20]):
        clean_row = [str(cell).strip().lower() if cell is not None else "" for cell in row]
        if search_term in clean_row:
            header_row_idx = i
            break
    
    if header_row_idx == -1:
        frappe.throw(_("Required header '{0}' not found.").format(FIELD_MAPPING["arn_no"]))
        
    headers = [str(h).strip() if h else "" for h in rows[header_row_idx]]
    all_data_rows = rows[header_row_idx + 1:]
    data_rows = [r for r in all_data_rows if any(c is not None and str(c).strip() != "" for c in r)]
    
    missing_headers = [header for header in FIELD_MAPPING.values() if header not in headers]
            
    return {
        "total_rows": len(data_rows),
        "missing_headers": missing_headers
    }

@frappe.whitelist()
def run_import_sample():
    frappe.enqueue(
        "credlawn.credlawn.doctype.import_dump.import_payout_dump.execute_import",
        queue="long",
        timeout=3600
    )
    return {"status": "success"}

def execute_import():
    try:
        import_doc = frappe.get_single("Import Dump")
        dump_till = getdate(import_doc.dump_till)
        
        # Calculate Payout Month (MMM-YY) from Import Dump
        payout_month = dump_till.strftime("%b-%y") if dump_till else ""
        
        rows = read_xlsx_file_from_attached_file(file_url=import_doc.attach_dump)
        if not rows: return
            
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
        counters = {"created": 0, "updated": 0, "processed": 0}
        
        # 5. Process Rows
        for row_idx, row in enumerate(data_rows):
            counters["processed"] += 1
            
            doc_data = {
                "doctype": "Payout Dump",
                "payout_month": payout_month,
                "record_type": import_doc.dump_type,
                "multiplier": 1 if import_doc.dump_type == "Payout" else -1
            }
            
            # Map Excel columns
            for i, label in enumerate(headers):
                if label in excel_to_field:
                    fname = excel_to_field[label]
                    val = row[i] if i < len(row) else None
                    if val is not None and str(val).strip().upper() == "#N/A": val = None
                    
                    if val and fname == "decision_date":
                        try: val = getdate(val)
                        except: pass
                    
                    doc_data[fname] = val

            # Slab Mapping Logic
            comp = str(doc_data.get("payout_computation", ""))
            if "100%" in comp: doc_data["payout_slab"] = 1.0
            elif "80%" in comp: doc_data["payout_slab"] = 0.8
            elif "20%" in comp: doc_data["payout_slab"] = 0.2

            arn_no = str(doc_data.get("arn_no", "")).strip().upper()
            if not arn_no: continue
            doc_data["arn_no"] = arn_no

            # Uniqueness Check (ARN + Month + Type)
            filters = {
                "arn_no": arn_no,
                "payout_month": payout_month,
                "record_type": import_doc.dump_type
            }

            try:
                existing_name = frappe.db.get_value("Payout Dump", filters)
                
                if existing_name:
                    doc = frappe.get_doc("Payout Dump", existing_name)
                    doc.update(doc_data)
                    doc.save(ignore_permissions=True)
                    counters["updated"] += 1
                else:
                    doc = frappe.get_doc(doc_data)
                    doc.insert(ignore_permissions=True)
                    counters["created"] += 1
            except Exception as e:
                frappe.log_error(f"Payout Row {row_idx + 2} Error: {str(e)}", "Payout Dump Import")
            
            # Real-time UI progress updates
            if counters["processed"] % 50 == 0 or counters["processed"] == total_rows:
                percentage = int((counters["processed"] / total_rows) * 100)
                msg = _("Payout: Processing {0} of {1}...").format(counters["processed"], total_rows)
                publish_progress(percentage, msg)
                
            if counters["processed"] % 200 == 0:
                frappe.db.commit()

        frappe.db.commit()
        publish_progress(100, _("Payout Import Finished! Created: {0}, Updated: {1}").format(counters["created"], counters["updated"]))

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Payout Dump Import Critical Failure")
        publish_progress(0, _("Payout Import failed: {0}").format(str(e)), failed=True)

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("payout_import_progress", {"percentage": percentage, "message": message, "failed": failed})
