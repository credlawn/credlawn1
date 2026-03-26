import frappe
from frappe.model.document import Document
from frappe.utils import getdate

class DSADump(Document):
	def autoname(self):
		self.name = self.arn_no

	def on_update(self):
		# Professional Sync: Prevents recursion and implements conflict-aware reconciliation
		sync_dsa_data(self)

def sync_dsa_data(doc):
	"""
	Main synchronization orchestrator for DSA Dump.
	Handles IPA enrichment, conflict resolution for bank statuses, and BPA payout sync.
	"""
	try:
		# 1. Fetch Existing metadata from IPA Records (Optimized Single Query)
		ipa_info = frappe.db.get_value(
			"IPA Records", 
			{"arn_no": doc.arn_no}, 
			["name", "employee_name", "employee_code", "mobile_no", "bank_status"], 
			as_dict=True
		)

		# 2. Enrich current DSA Dump with Employee data
		# Use db_set to avoid triggering on_update again (Infinite Loop Prevention)
		doc.db_set({
			"employee_name": ipa_info.get("employee_name") if ipa_info else None,
			"employee_code": ipa_info.get("employee_code") if ipa_info else None,
			"mobile_no": ipa_info.get("mobile_no") if ipa_info else None
		}, commit=False)

		# If no IPA record exists for this ARN, we cannot perform external sync
		if not ipa_info:
			return

		# 3. IPA Sync & Conflict Resolution Logic
		ipa_updates = {}
		current_ipa_status = ipa_info.get("bank_status")

		# Rule: Mark as validated if a match is found (Prevents redundant re-mapping)
		ipa_updates["dsa_dump_validated"] = "Yes"

		# Rule: Protect 'Approve' status from being overwritten by 'Decline' or older data
		if current_ipa_status in [None, "", "Decline"]:
			if doc.final_decision in ["Approve", "Decline"]:
				ipa_updates.update({
					"bank_status": doc.final_decision,
					"bank_status_date": doc.final_decision_date,
					"bank_status_month": doc.decision_month
				})

		# 4. Payout Sync to BPA Records (Approve only)
		if doc.final_decision == "Approve":
			ipa_updates["final_approved_count_verified"] = "Yes"
			process_bpa_payout_sync(doc, ipa_info)

		# Save all IPA updates in one optimized operation
		if ipa_updates:
			frappe.db.set_value("IPA Records", ipa_info.name, ipa_updates, update_modified=True)

	except Exception as e:
		# Fail-safe: Log error but ensure the main import process continues
		frappe.log_error(f"DSA Sync Sync Error (ARN {doc.arn_no}): {str(e)}", "DSA Sync Architecture")

def process_bpa_payout_sync(doc, ipa_info):
	"""Manages upsert logic for BPA Records with detailed DSA audit fields."""
	try:
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
			# Employee enrichment
			"employee_name": ipa_info.get("employee_name"),
			"employee_code": ipa_info.get("employee_code"),
			"mobile_no": ipa_info.get("mobile_no")
		}

		# BPA Records uses arn_no as its primary name (ID)
		if frappe.db.exists("BPA Records", doc.arn_no):
			bpa_doc = frappe.get_doc("BPA Records", doc.arn_no)
			bpa_doc.update(bpa_mapping)
			bpa_doc.save(ignore_permissions=True)
		else:
			bpa_doc = frappe.get_doc(bpa_mapping)
			bpa_doc.insert(ignore_permissions=True)
			
	except Exception as e:
		frappe.log_error(f"DSA-BPA Payout Sync Error (ARN {doc.arn_no}): {str(e)}", "BPA Payout Logic")
