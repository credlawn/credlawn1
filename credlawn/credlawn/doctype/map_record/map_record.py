import frappe
import json
from frappe import _
from frappe.model.document import Document
from frappe.utils.xlsxutils import read_xlsx_file_from_attached_file, make_xlsx
from frappe.utils import now_datetime

class MapRecord(Document):
    pass

@frappe.whitelist()
def clear_mapping_data():
    """Clears all configuration fields and permanently deletes attached files."""
    try:
        doc = frappe.get_single("Map Record")
        
        # Identify and delete physical files from storage
        for field in ["source_file", "mapped_file"]:
            file_url = doc.get(field)
            if file_url:
                # Find File document name based on URL
                file_name = frappe.db.get_value("File", {"file_url": file_url}, "name")
                if file_name:
                    frappe.delete_doc("File", file_name, ignore_permissions=True)

        # Reset all fields to clean state
        doc.source_file = None
        doc.mapped_file = None
        doc.target_doctype = None
        doc.excel_lookup_column = None
        doc.doctype_lookup_field = None
        doc.fields_to_extract = None
        doc.status_summary = "<div style='color: gray;'>Data cleared. Ready for new mapping.</div>"
        
        doc.save(ignore_permissions=True)
        frappe.db.commit()
        return _("Cleanup successful. Files deleted and form reset.")
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Map Record Clear Error")
        frappe.throw(_("Failed to clear data: {0}").format(str(e)))

@frappe.whitelist()
def get_excel_headers():
    """Returns the top row of the attached Excel file as a list of strings."""
    try:
        doc = frappe.get_single("Map Record")
        if not doc.source_file:
            return []
        
        rows = read_xlsx_file_from_attached_file(file_url=doc.source_file)
        if rows and len(rows) > 0:
            return [str(h).strip() for h in rows[0] if h is not None]
    except Exception as e:
        frappe.log_error(f"Header Parse Error: {str(e)}", "Map Record")
    return []

@frappe.whitelist()
def get_doctype_fields(dt):
    """Returns a list of searchable fieldnames for a given DocType."""
    try:
        # Check permission for the target DocType
        if not frappe.has_permission(dt, "read"):
            return []
            
        meta = frappe.get_meta(dt)
        # We include 'name' as it's the primary lookup field usually
        fields = ["name"] 
        fields += [
            d.fieldname 
            for d in meta.fields 
            if d.fieldtype not in ["Section Break", "Column Break", "Tab Break", "Table", "Heading", "HTML"]
        ]
        return sorted(list(set(fields)))
    except Exception as e:
        frappe.log_error(f"DocType Field Fetch Error: {str(e)}", "Map Record")
        return []

@frappe.whitelist()
def start_mapping(target_dt, excel_col, dt_field, extract_fields):
    """Enqueues the Excel Enrichment process."""
    try:
        frappe.enqueue(
            "credlawn.credlawn.doctype.map_record.map_record.process_data",
            target_dt=target_dt,
            excel_col=excel_col,
            dt_field=dt_field,
            extract_fields=json.loads(extract_fields),
            queue="long",
            timeout=3600
        )
        return _("Enrichment started in the background. Please wait...")
    except Exception as e:
        frappe.throw(_("Failed to start process: {0}").format(str(e)))

def process_data(target_dt, excel_col, dt_field, extract_fields):
    """
    Main Background Job:
    1. Reads source Excel.
    2. Identifies matching records in Frappe.
    3. Merges data and creates a new Excel file.
    """
    try:
        doc = frappe.get_single("Map Record")
        rows = read_xlsx_file_from_attached_file(file_url=doc.source_file)
        if not rows: 
            return

        headers = [str(h).strip() for h in rows[0]]
        try:
            lookup_idx = headers.index(excel_col)
        except ValueError:
            frappe.log_error(f"Column '{excel_col}' not found in Excel headers", "Map Record processing")
            return

        data_rows = rows[1:]
        # Get unique lookup values to minimize DB queries
        lookup_values = list(set([str(r[lookup_idx]).strip() for r in data_rows if r[lookup_idx]]))
        
        if not lookup_values:
            frappe.log_error("No lookup values found in the Excel column", "Map Record processing")
            return

        # Fetch records. Sorted by modified DESC ensures the latest record is processed first
        # during the dictionary build phase (latest wins).
        db_fields = list(set(extract_fields + [dt_field]))
        records = frappe.get_all(
            target_dt, 
            fields=db_fields, 
            filters={dt_field: ["in", lookup_values]},
            order_by="modified desc"
        )
        
        # Build lookup table { mobile_no: { 'lead_status': 'Called', ... } }
        mapping_dict = {}
        for r in records:
            key = str(r.get(dt_field)).strip()
            if key not in mapping_dict:
                mapping_dict[key] = r

        # Construct New Excel Data
        new_headers = headers + extract_fields
        new_rows = [new_headers]
        
        for r in data_rows:
            new_row = list(r)
            # Ensure the row has same length as header even if empty cells at end
            if len(new_row) < len(headers):
                new_row.extend([None] * (len(headers) - len(new_row)))
            
            val = str(r[lookup_idx]).strip() if lookup_idx < len(r) else ""
            
            match = mapping_dict.get(val)
            for f in extract_fields:
                val_to_append = match.get(f) if match else ""
                # Trim whitespace if the value is a string
                if isinstance(val_to_append, str):
                    val_to_append = val_to_append.strip()
                new_row.append(val_to_append)
            
            new_rows.append(new_row)

        # Generate and save the file
        xlsx_content = make_xlsx(new_rows, "Mapped Data")
        
        file_name = f"Enriched_{target_dt}_{excel_col}.xlsx".replace(" ", "_")
        
        # Clear previous mapping result if any
        if doc.mapped_file:
            frappe.db.sql("DELETE FROM `tabFile` WHERE attached_to_name='Map Record' AND file_url=%s", doc.mapped_file)

        file_doc = frappe.get_doc({
            "doctype": "File",
            "file_name": file_name,
            "attached_to_doctype": "Map Record",
            "attached_to_name": "Map Record",
            "content": xlsx_content.getvalue() if hasattr(xlsx_content, 'getvalue') else xlsx_content,
            "is_private": 1
        })
        file_doc.save(ignore_permissions=True)
        
        # Update Single DocType
        doc.mapped_file = file_doc.file_url
        doc.status_summary = f"<div style='color: green;'><b>Success!</b> Checked {len(data_rows)} rows. Matched {len(mapping_dict)} unique records from {target_dt}.</div>"
        doc.save(ignore_permissions=True)
        frappe.db.commit()
        
        frappe.publish_realtime("map_record_progress", {"status": "success", "file_url": file_doc.file_url})

    except Exception as e:
        error_msg = frappe.get_traceback()
        frappe.log_error(error_msg, "Map Record Progress Error")
        frappe.publish_realtime("map_record_progress", {
            "status": "failed", 
            "message": str(e)
        })
