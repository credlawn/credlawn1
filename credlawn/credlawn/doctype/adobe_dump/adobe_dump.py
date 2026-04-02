import frappe
from frappe.model.document import Document
from frappe.utils import getdate

class AdobeDump(Document):
	def autoname(self):
		self.name = self.arn_no

	def on_update(self):
		# Professional Sync: Prevents recursion and optimizes performance
		sync_and_enrich_data(self)

def sync_and_enrich_data(doc):
	"""
	Coordinates data enrichment for Adobe Dump and synchronization with IPA/BPA Records.
	Ensures all 'Approve' records reach BPA even without IPA mapping.
	"""
	try:
		# 1. Fetch Enrichment Data from IPA Records (Optimized Single Query)
		ipa_info = frappe.db.get_value(
			"IPA Records", 
			{"arn_no": doc.arn_no}, 
			["name", "employee_name", "employee_code", "mobile_no"], 
			as_dict=True
		)

		# 2. Prepare Internal Fields (Enriching the current Adobe Dump record)
		decision_date = getdate(doc.final_decision_date) if doc.final_decision_date else None
		decision_month = decision_date.strftime("%b-%y") if decision_date else ""

		# Use db_set to update internal fields without triggering on_update again (Prevents Loop)
		# Fallback to 'Unmapped' if IPA entry doesn't exist yet
		doc.db_set({
			"decision_month": decision_month,
			"employee_name": ipa_info.get("employee_name") if ipa_info else "Unmapped",
			"employee_code": ipa_info.get("employee_code") if ipa_info else "0",
			"mobile_no": ipa_info.get("mobile_no") if ipa_info else ""
		}, commit=False)

		# 3. Payout Sync to BPA Records (Approve only) 
		# This happens regardless of IPA mapping existence (User Request)
		if doc.final_decision == "Approve":
			process_bpa_sync(doc, ipa_info or {}, decision_date, decision_month)

		# 4. Supplemental IPA Records Logic (Only if IPA Record exists)
		if ipa_info:
			ipa_updates = {"adobe_dump_validated": "Yes"}

			if doc.final_decision in ["Approve", "Decline"]:
				ipa_updates.update({
					"bank_status": doc.final_decision,
					"bank_status_date": decision_date,
					"bank_status_month": decision_month
				})

			if doc.final_decision == "Approve":
				ipa_updates["final_approved_count_verified"] = "Yes"

			# Execute IPA record updates in one go
			frappe.db.set_value("IPA Records", ipa_info.name, ipa_updates, update_modified=True)

	except Exception as e:
		# Production Safety: Log but don't break the main document save/import
		frappe.log_error(f"Adobe Sync Error (ARN {doc.arn_no}): {str(e)}", "Data Sync Architecture")

def process_bpa_sync(doc, ipa_info, decision_date, decision_month):
	"""Creates or updates records in BPA Records for payout reconciliation."""
	try:
		bpa_data = {
			"doctype": "BPA Records",
			"arn_no": doc.arn_no,
			"customer_name": doc.customer_name,
			"dsa_code": doc.dsa_code,
			"sm_code": doc.sm_code,
			"lc1_code": doc.lc1_code,
			"lc2_code": doc.lc2_code,
			"arn_date": doc.arn_date,
			"customer_type": doc.customer_type,
			"decision_date": decision_date,
			"decision_month": decision_month,
			"promo_code": doc.promo_code,
			"product": doc.product_description,
			"card_type": doc.card_type,
			"card_activation_status": doc.card_activation_status,
			# Employee matching (Fallback to Unmapped placeholder)
			"employee_name": ipa_info.get("employee_name") if ipa_info else "Unmapped",
			"employee_code": ipa_info.get("employee_code") if ipa_info else "0",
			"mobile_no": ipa_info.get("mobile_no") if ipa_info else ""
		}

		if doc.kyc_type and str(doc.kyc_type).strip().lower() == "biokyc":
			bpa_data["biokyc"] = "Yes"

		# BPARecords uses arn_no as its name (ID)
		if frappe.db.exists("BPA Records", doc.arn_no):
			bpa_doc = frappe.get_doc("BPA Records", doc.arn_no)
			bpa_doc.update(bpa_data)
			bpa_doc.save(ignore_permissions=True)
		else:
			bpa_doc = frappe.get_doc(bpa_data)
			bpa_doc.insert(ignore_permissions=True)
			
	except Exception as e:
		frappe.log_error(f"BPA Async Error (ARN {doc.arn_no}): {str(e)}", "BPA Records Sync")
