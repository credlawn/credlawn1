import frappe
import datetime
from frappe import _
from frappe.utils.xlsxutils import read_xlsx_file_from_attached_file
from frappe.utils import getdate
import math

def parse_date(val):
    """
    Robustly parses dates from Excel. 
    Handles: Python datetime, Excel serial (float/int), and Strings.
    """
    if not val or str(val).strip().upper() in ["", "#N/A", "NA", "N/A", "NULL", "NONE", "NAN"]:
        return None
        
    try:
        if isinstance(val, (int, float)) or (isinstance(val, str) and val.replace('.','',1).isdigit()):
            dt = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=float(val))
            return getdate(dt)
    except:
        pass

    if isinstance(val, str):
        val = val.strip().replace("  ", " ")
        if not val: return None

        patterns = [
            "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", 
            "%m-%d-%Y", "%m/%d/%Y",             
            "%Y-%m-%d", "%Y/%m/%d"              
        ]
        for fmt in patterns:
            try:
                return getdate(datetime.datetime.strptime(val, fmt))
            except:
                continue
    
    try:
        return getdate(val)
    except:
        return None


FIELD_MAPPING = {
    "arn_no": "APPL_REF",
    "customer_name": "FULL_NAME",
    "product": "PRODUCT_DESC",
    "city": "CITY",
    "employer": "EMPLOYER",
    "final_decision_date": "DECISIN_DT",
    "seg_id": "SEG_ID",
    "activation_status": "Activation Status",
    "promo_code": "PROMOCODE_NEW",
    "dsa_code": "Final Team Code",
    "lc1_code": "LC1CODE_1",
    "lc2_code": "LC2CODE",
    "sm_code": "SMCODE",
    "decline_code": "MISDECCODE",
    "decline_category": "DECLINE_CATEGORY",
    "decline_description": "DECLINE_DESCRIPTION"
}

# Priority Ranking for Activation Status (Higher = More Active/Final)
ACTIVATION_RANK = {
    "Inactive": 1,
    "V+ Active": 2,
    "Txn Active": 3,
    "Txn Active - Rs 100": 4,
    "Card closed": 5 # Always allowed as it's a terminal/special state
}

def normalize_status(status):
    """
    Robust standardization for Activation Status:
    - Strips, removes underscores, reduces spaces
    - Maps all known variants (V+Active, V+ Active, etc.) to one canonical string
    """
    if not status or str(status).strip().upper() in ["", "#N/A", "NA", "N/A", "NULL", "NONE", "NAN"]:
        return "Inactive"
    
    # 1. Standardize formatting (Uppercase, No underscores, Single spaces)
    s = str(status).strip().upper().replace("_", " ")
    s = " ".join(s.split())
    
    # 2. Canonical Map
    # Key = standardized input, Value = exact string expected in DB/Dropdown
    lookup = {
        "V+ACTIVE": "V+ Active",
        "V+ ACTIVE": "V+ Active",
        "TXN ACTIVE": "Txn Active",
        "TXNACTIVE": "Txn Active",
        "INACTIVE": "Inactive",
        "CARD CLOSED": "Card closed",
        "CARDCLOSED": "Card closed"
    }
    
    if s in lookup:
        return lookup[s]
    
    # 3. Special substring matches
    if "TXN ACTIVE" in s and "100" in s:
        return "Txn Active - Rs 100"
    
    # 4. Fallback to title case for unknown but clean strings
    return s.title()

def parse_date_from_arn(arn):
    """
    Extracts Date from ARN Number:
    Example: D26A31... (2026, Jan, 31)
    """
    if not arn or len(arn) < 6:
        return None
    try:
        arn = str(arn).strip().upper()
        year = int(f"20{arn[1:3]}")
        month = ord(arn[3]) - ord('A') + 1
        day = int(arn[4:6])
        return datetime.date(year, month, day)
    except:
        return None

FILTER_COLUMN = "SETUP_STAT"
FILTER_VALUES = ["A", "D"]

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
        if search_term in clean_row or FILTER_COLUMN.lower() in clean_row:
            header_row_idx = i
            break
    
    if header_row_idx == -1:
        frappe.throw(_("Required header '{0}' or '{1}' not found.").format(FIELD_MAPPING["arn_no"], FILTER_COLUMN))
        
    headers = [str(h).strip() if h else "" for h in rows[header_row_idx]]
    all_data_rows = rows[header_row_idx + 1:]
    
    data_rows = []
    stat_idx = headers.index(FILTER_COLUMN) if FILTER_COLUMN in headers else -1

    for r in all_data_rows:
        if not any(c is not None and str(c).strip() != "" for c in r):
            continue
        
        if stat_idx != -1:
            stat_val = str(r[stat_idx]).strip().upper() if r[stat_idx] else ""
            if stat_val in FILTER_VALUES:
                data_rows.append(r)

    missing_headers = [header for header in FIELD_MAPPING.values() if header not in headers]
    if FILTER_COLUMN not in headers:
        missing_headers.append(FILTER_COLUMN)
            
    return {
        "total_rows": len(data_rows),
        "missing_headers": missing_headers
    }

@frappe.whitelist()
def run_import_sample():
    frappe.enqueue(
        "credlawn.credlawn.doctype.import_dump.import_dsa_dump.execute_import",
        queue="long",
        timeout=3600
    )
    return {"status": "success"}

def execute_import():
    try:
        import_doc = frappe.get_single("Import Dump")
        dump_till = getdate(import_doc.dump_till)
        
        rows = read_xlsx_file_from_attached_file(file_url=import_doc.attach_dump)
        if not rows: return
            
        header_row_idx = -1
        search_term = str(FIELD_MAPPING["arn_no"]).strip().lower()
        for i, row in enumerate(rows[:20]):
            if search_term in [str(cell).strip().lower() if cell is not None else "" for cell in row] \
               or FILTER_COLUMN.lower() in [str(cell).strip().lower() if cell is not None else "" for cell in row]:
                header_row_idx = i
                break
        
        headers = [str(h).strip() if h else "" for h in rows[header_row_idx]]
        all_data_rows = rows[header_row_idx + 1:]
        
        excel_to_field = {v: k for k, v in FIELD_MAPPING.items()}
        stat_idx = headers.index(FILTER_COLUMN) if FILTER_COLUMN in headers else -1

        existing_records = {
            d.arn_no.strip().upper(): {"name": d.name, "date": d.dsa_dump_date}
            for d in frappe.get_all("DSA Dump", fields=["name", "arn_no", "dsa_dump_date"]) 
            if d.arn_no
        }
        
        counters = {"created": 0, "updated": 0, "skipped": 0, "processed": 0}
        skip_log = {}
        error_details = []

        def log_reason(reason):
            skip_log[reason] = skip_log.get(reason, 0) + 1

        valid_rows = []
        
        for r in all_data_rows:
            if any(c is not None and str(c).strip() != "" for c in r):
                if stat_idx != -1:
                    stat_val = str(r[stat_idx]).strip().upper() if r[stat_idx] else ""
                    if stat_val in FILTER_VALUES:
                        valid_rows.append(r)

        total_rows = len(valid_rows)
        
        for row_idx, row in enumerate(valid_rows):
            counters["processed"] += 1
            doc_data = {"doctype": "DSA Dump", "dsa_dump_date": dump_till}
            
            for i, label in enumerate(headers):
                if label in excel_to_field:
                    fname = excel_to_field[label]
                    val = row[i] if i < len(row) else None
                    if val is not None and str(val).strip().upper() == "#N/A":
                        if fname != "activation_status":
                            val = None
                    
                    if fname == "activation_status":
                        val = normalize_status(val)
                    elif val and isinstance(val, str):
                        val = val.strip()

                    if val and fname == "final_decision_date":
                        val = parse_date(val)
                        if val:
                            doc_data["decision_month"] = val.strftime("%b-%y")
                    
                    doc_data[fname] = val
                    
                elif label == FILTER_COLUMN:
                    stat_val = str(row[i]).strip().upper() if row[i] else ""
                    if stat_val == "A":
                        doc_data["final_decision"] = "Approve"
                    elif stat_val == "D":
                        doc_data["final_decision"] = "Decline"

            arn_no = str(doc_data.get("arn_no", "")).strip().upper()
            if not arn_no: 
                log_reason("No ARN")
                continue
            doc_data["arn_no"] = arn_no

            # REFINEMENT: Activation Status State-Transition Logic
            new_status = doc_data.get("activation_status") # Normalized at source
            if arn_no in existing_records:
                # 1. Fetch current status from DB (Normalize for comparison)
                old_status = normalize_status(frappe.db.get_value("DSA Dump", existing_records[arn_no]["name"], "activation_status"))
                
                # 2. Guard: Never overwrite with Blank/Null
                if not new_status:
                    doc_data.pop("activation_status", None)
                else:
                    # 3. Rule: Check Priority (Prevent Downgrades)
                    if new_status == "Card closed":
                        pass
                    else:
                        new_rank = ACTIVATION_RANK.get(new_status, 0)
                        old_rank = ACTIVATION_RANK.get(old_status, 0)
                        
                        if new_rank < old_rank:
                            doc_data.pop("activation_status", None)
                        else:
                            # New status is higher/equal, keep the normalized value in doc_data
                            pass

            arn_date = parse_date_from_arn(arn_no)
            if arn_date:
                doc_data["arn_date"] = arn_date

            try:
                if arn_no in existing_records:
                    existing = existing_records[arn_no]
                    
                    existing_date = getdate(existing.get("date"))
                    current_dump_date = getdate(dump_till)
                    
                    if existing_date and current_dump_date and existing_date > current_dump_date:
                        counters["skipped"] += 1
                        log_reason("Older Data")
                        continue

                    doc = frappe.get_doc("DSA Dump", existing["name"])
                    doc.update(doc_data)
                    doc.save(ignore_permissions=True)
                    counters["updated"] += 1
                else:
                    doc = frappe.get_doc(doc_data)
                    doc.insert(ignore_permissions=True)
                    existing_records[arn_no] = {"name": doc.name, "date": dump_till}
                    counters["created"] += 1
            except Exception as e:
                log_reason("Exec Error")
                error_details.append(f"DSA Row {row_idx + 2} (ARN: {arn_no}) Error: {str(e)}")
            
            if counters["processed"] % 50 == 0 or counters["processed"] == total_rows:
                percentage = int((counters["processed"] / total_rows) * 100)
                msg = _("DSA: Processing {0} of {1}...").format(counters["processed"] , total_rows)
                publish_progress(percentage, msg)
                
            if counters["processed"] % 200 == 0:
                frappe.db.commit()

        frappe.db.commit()
        summary = _("DSA Import Finished! Created: {0}, Updated: {1}, Skipped: {2}").format(
            counters["created"], counters["updated"], counters["skipped"]
        )

        if skip_log or error_details:
            details_str = "--- Skip Summary ---\n"
            for k, v in skip_log.items():
                details_str += f"{k}: {v} records\n"
            
            if error_details:
                details_str += "\n--- Row Errors ---\n"
                details_str += "\n".join(error_details)
            
            frappe.log_error(
                title=f"DSA Import Summary ({counters['processed']} rows)", 
                message=details_str
            )

        publish_progress(100, summary)

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "DSA Dump Import Critical Failure")
        publish_progress(0, _("DSA Import failed: {0}").format(str(e)), failed=True)

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("dsa_import_progress", {"percentage": percentage, "message": message, "failed": failed})
