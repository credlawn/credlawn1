# Copyright (c) 2026, Credlawn India and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class BPARecords(Document):
	def autoname(self):
		self.name = self.arn_no
