import frappe
from frappe import _

@frappe.whitelist()
def get_decision_months():
    try:
        data = frappe.db.sql("SELECT DISTINCT decision_month FROM `tabBPA Records` WHERE decision_month IS NOT NULL AND decision_month != ''", as_dict=True)
        return [d.decision_month for d in data]
    except Exception as e:
        frappe.log_error(f"Failed to fetch decision months: {str(e)}", "BPA Records")
        return []

@frappe.whitelist()
def execute_calculation(decision_month=None):
    """
    Payout Calculation Logic.
    Triggered from 'Calculate Payout' button on BPA Records list view.
    """
    try:
        if not decision_month:
            frappe.throw(_("Decision Month is required to calculate payout."))
            
        all_records = frappe.get_all(
            "BPA Records",
            filters={"decision_month": decision_month},
            fields=["name", "customer_type", "product", "biokyc", "card_activation_status", "decision_date"],
            order_by="decision_date asc"
        )

        total_cards = len(all_records)
        if total_cards == 0:
            frappe.throw(_("No records found for the selected Decision Month."))
            
        wl_records = [r for r in all_records if r.customer_type and str(r.customer_type).strip().upper() == "WL"]
        wl_count = len(wl_records)
        
        # Calculate WL allowance
        wl_ratio = wl_count / total_cards
        limit_wl_cards = round(total_cards * 0.25)
        
        # Determine WL deduction tier
        if wl_ratio > 0.35:
            wl_deduction = 500.0
        elif wl_ratio > 0.25:
            wl_deduction = 250.0
        else:
            wl_deduction = 0.0
            
        # Get mapping of all products from Card Level Payout
        product_payouts = frappe.get_all("Card Level Payout", fields=["product", "pre_gst_amount"])
        def normalize_product(prod_str):
            if not prod_str: return ""
            return str(prod_str).strip().upper().replace(" ", "")
            
        payout_map = {normalize_product(p.product): p.pre_gst_amount for p in product_payouts}
        
        calculated_count = 0
        wl_processed_count = 0
        
        for record in all_records:
            updates = {}
            
            # --- 1. KYC Deduction & Payout Slab Setup ---
            kyc_deduction = 320.0 if (record.biokyc and str(record.biokyc).strip().lower() == "yes") else 0.0
            updates["kyc_deduction"] = kyc_deduction
            
            status = str(record.card_activation_status).strip().lower() if record.card_activation_status else ""
            if status in ["inactive", "card closed", ""]:
                payout_slab = 0.0
            elif status == "txn active - rs 100":
                payout_slab = 1.0
            elif status in ["txn active", "v+ active"]:
                payout_slab = 0.8
            else:
                payout_slab = 0.0
            updates["payout_slab"] = payout_slab
                
            # --- 2. Base PO Calculation ---
            is_wl = record.customer_type and str(record.customer_type).strip().upper() == "WL"
            
            if is_wl:
                wl_processed_count += 1
                if wl_processed_count <= limit_wl_cards:
                    base_po = 1695.0
                else:
                    base_po = 1695.0 - wl_deduction
            else:
                # Non-WL Logic (Lookup Card Level Payout by normalized Product)
                norm_prod = normalize_product(record.product)
                base_po = float(payout_map.get(norm_prod, 0.0))
                
            updates["base_po"] = float(base_po)
            
            # --- 3. Final Actual PO Calculation ---
            # Formula: ((base_po - kyc_deduction) * payout_slab) - (150 * payout_slab)
            actual_po = ((base_po - kyc_deduction) * payout_slab) - (150.0 * payout_slab)
            updates["actual_po"] = float(actual_po)
            
            frappe.db.set_value("BPA Records", record.name, updates, update_modified=True)
            calculated_count += 1
            
        frappe.db.commit()
        return _("Successfully calculated complex payout rules for {0} records in {1}.").format(calculated_count, decision_month)
        
    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Payout Calculation Error")
        frappe.throw(_("Error during calculation: {0}").format(str(e)))
