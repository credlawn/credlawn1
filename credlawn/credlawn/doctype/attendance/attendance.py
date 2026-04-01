import frappe
from frappe.model.document import Document

class Attendance(Document):
	def on_update(self):
		# 1. Prevent loop if update is coming from the PB sync background job
		if self.flags.from_pb_sync:
			return
		
		# 2. Only push to Pocketbase if approved_status has actually changed
		if self.has_value_changed('approved_status') and self.pb_id:
			# Enqueue to keep performace high and UI responsive
			frappe.enqueue(
				'credlawn.credlawn.doctype.attendance.sync_atn.push_status_to_pb',
				doc_name=self.name,
				now=frappe.flags.in_test
			)
