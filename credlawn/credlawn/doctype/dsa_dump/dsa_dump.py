import frappe
from frappe.model.document import Document
from frappe.utils import getdate

class DSADump(Document):
	def autoname(self):
		self.name = self.arn_no

	def on_update(self):
		# Professional Sync: Prevents recursion and optimizes performance
		sync_dsa_data(self)

def sync_dsa_data(doc):
	"""
	Main synchronization orchestrator for DSA Dump.
	Ensures BPA payout record is created even if IPA (Employee) mapping is missing.
	"""
	try:
		# 1. Fetch Existing metadata from IPA Records (Optimized Single Query)
		ipa_info = frappe.db.get_value(
			"IPA Records", 
			{"arn_no": doc.arn_no}, 
			["name", "employee_name", "employee_code", "mobile_no", "bank_status"], 
			as_dict=True
		)

		# 2. Enrich current DSA Dump with Employee data (Fallback to Unmapped)
		# Use db_set to avoid triggering on_update again (Infinite Loop Prevention)
		doc.db_set({
			"employee_name": ipa_info.get("employee_name") if ipa_info else "Unmapped",
			"employee_code": ipa_info.get("employee_code") if ipa_info else "0",
			"mobile_no": ipa_info.get("mobile_no") if ipa_info else ""
		}, commit=False)

		# 3. Payout Sync to BPA Records (Approve only) 
		# This happens regardless of IPA mapping existence (User Request)
		if doc.final_decision == "Approve":
			process_bpa_payout_sync(doc, ipa_info or {})

		# 4. Supplemental IPA Records Logic (Only if IPA Record exists)
		if ipa_info:
			ipa_updates = {}
			current_ipa_status = ipa_info.get("bank_status")

			# Rule: Mark as validated if a match is found
			ipa_updates["dsa_dump_validated"] = "Yes"

			# Rule: Protect 'Approve' status from being overwritten
			if current_ipa_status in [None, "", "Decline"]:
				if doc.final_decision in ["Approve", "Decline"]:
					ipa_updates.update({
						"bank_status": doc.final_decision,
						"bank_status_date": doc.final_decision_date,
						"bank_status_month": doc.decision_month
					})

			if doc.final_decision == "Approve":
				ipa_updates["final_approved_count_verified"] = "Yes"

			# Save all IPA updates in one optimized operation
			if ipa_updates:
				frappe.db.set_value("IPA Records", ipa_info.name, ipa_updates, update_modified=True)

	except Exception as e:
		# Fail-safe logic
		frappe.log_error(f"DSA Sync Error (ARN {doc.arn_no}): {str(e)}", "DSA Sync Architecture")

def process_bpa_payout_sync(doc, ipa_info):
	"""Manages selective upsert logic for BPA Records."""
	try:
		# Rule: Selective update for existing, Full create for new
		existing_bpa = frappe.db.get_value("BPA Records", doc.arn_no, ["name", "employee_name"], as_dict=True)

		if existing_bpa:
			# Case 1: UPDATE logic (Selective DSA Tracking Only)
			bpa_updates = {
				"dsa_dump_verified": "Yes",
				"seg_id": doc.seg_id,
				"product_in_dsa_dump": doc.product,
				"activation_status": doc.activation_status,
				"dsa_dump_date": doc.dsa_dump_date,
			}

			# Employee Rule: Only update if current BPA is 'Unmapped'
			if existing_bpa.get("employee_name") == "Unmapped":
				bpa_updates.update({
					"employee_name": ipa_info.get("employee_name") if ipa_info else "Unmapped",
					"employee_code": ipa_info.get("employee_code") if ipa_info else "0",
					"mobile_no": ipa_info.get("mobile_no") if ipa_info else ""
				})
			
			frappe.db.set_value("BPA Records", doc.arn_no, bpa_updates, update_modified=True)
		
		else:
			# Case 2: CREATE logic (Full mapping)
			bpa_mapping = {
				"doctype": "BPA Records",
				"arn_no": doc.arn_no,
				"customer_name": doc.customer_name,
				"dsa_code": doc.dsa_code,
				"sm_code": doc.sm_code,
				"lc1_code": doc.lc1_code,
				"lc2_code": doc.lc2_code,
				"arn_date": doc.arn_date,
				"decision_date": doc.final_decision_date,
				"decision_month": doc.decision_month,
				"promo_code": doc.promo_code,
				"product": doc.product,
				"card_activation_status": doc.activation_status,
				# DSA Specific Audit Fields
				"dsa_dump_verified": "Yes",
				"seg_id": doc.seg_id,
				"product_in_dsa_dump": doc.product,
				"activation_status": doc.activation_status,
				"dsa_dump_date": doc.dsa_dump_date,
				# Employee enrichment (Fallback to Unmapped)
				"employee_name": ipa_info.get("employee_name") if ipa_info else "Unmapped",
				"employee_code": ipa_info.get("employee_code") if ipa_info else "0",
				"mobile_no": ipa_info.get("mobile_no") if ipa_info else ""
			}
			# BPARecords uses arn_no as name (ID)
			bpa_doc = frappe.get_doc(bpa_mapping)
			bpa_doc.insert(ignore_permissions=True)
			
	except Exception as e:
		frappe.log_error(f"DSA-BPA Payout Sync Error (ARN {doc.arn_no}): {str(e)}", "BPA Payout Logic")
