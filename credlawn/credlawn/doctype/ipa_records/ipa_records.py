# Copyright (c) 2026, Credlawn India and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class IPARecords(Document):
	def autoname(self):
		if self.arn_no:
			self.name = self.arn_no.upper()

	def validate(self):
		if self.customer_name:
			self.customer_name = self.customer_name.upper()

		if self.arn_date:
			from frappe.utils import getdate
			# Convert to MMM-YY format like Jan-25
			self.arn_month = getdate(self.arn_date).strftime("%b-%y")

	def on_update(self):
		# Auto-rename the document if arn_no is changed to keep ID in sync
		if self.arn_no and self.name != self.arn_no.upper():
			import frappe
			frappe.rename_doc(self.doctype, self.name, self.arn_no.upper(), force=True)
