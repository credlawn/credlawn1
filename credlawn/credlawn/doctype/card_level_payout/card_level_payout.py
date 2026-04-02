# Copyright (c) 2026, Credlawn India and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class CardLevelPayout(Document):
	def validate(self):
		gross_amount = frappe.utils.flt(self.gross_amount)
		if gross_amount:
			self.pre_gst_amount = gross_amount / 1.18
			self.gst_amount = gross_amount - self.pre_gst_amount
		else:
			self.pre_gst_amount = 0.0
			self.gst_amount = 0.0
