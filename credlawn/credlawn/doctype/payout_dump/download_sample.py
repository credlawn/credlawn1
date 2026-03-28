import frappe
from frappe import _
from frappe.utils.xlsxutils import make_xlsx
from credlawn.credlawn.doctype.import_dump.import_payout_dump import FIELD_MAPPING

@frappe.whitelist()
def download_sample_payout_excel():
    """Generates a sample Excel file with headers matching the current Payout Dump mapping."""
    # 1. Get headers from the central mapping defined in import_payout_dump.py
    headers = list(FIELD_MAPPING.values())
    
    # 2. Add a sample data row to guide the user on expected formats
    sample_row = [
        "D25J24590063S0DP", # APPLNREF (ARN No)
        "TEAM001",         # TEAMCD (DSA Code)
        "SM01",            # SM (SM Code)
        "LC01",            # LC (LC1 Code)
        "LG01",            # LG (LC2 Code)
        "2026-03-26",      # DECISIN_DT (Decision Date)
        "Credit Card",     # Des (Product)
        "FT Activation 100% to be Paid", # Computation (Handles Slab 1.0, 0.8, 0.2)
        "1500",            # Final Rate (Final Amount)
        "Mumbai",          # CITY (City)
        "Maharashtra",     # State (State)
        "IndusInd Bank",   # EMPLOYER (Employer)
        "Market 1",        # Market (Market)
        "March",           # Month (Month)
        "Classic",         # Classification (Classification)
        "Physical",        # Activation Type (Activation Type)
        "1500",            # 100% Payout
        "1200",            # 80% Payout
        "300"              # 20% Payout
    ]
    
    # Ensure sample row matches header count in case of future mapping updates
    if len(sample_row) < len(headers):
        sample_row += [""] * (len(headers) - len(sample_row))
    
    data = [headers, sample_row]
    
    # 3. Create XLSX binary stream using Frappe's utility
    xlsx_file = make_xlsx(data, "Payout_Dump_Sample")
    
    # 4. Prepare Frappe response for direct file download
    frappe.response['filename'] = "Payout_Dump_Sample.xlsx"
    frappe.response['filecontent'] = xlsx_file.getvalue()
    frappe.response['type'] = "binary"
