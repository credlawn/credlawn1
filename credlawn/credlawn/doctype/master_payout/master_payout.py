# Copyright (c) 2026, Credlawn India and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import getdate


class MasterPayout(Document):
	def validate(self):
		self.set_sourcing_month()
		self.set_prime_month()
		self.set_po_type()
		self.set_employee_details()

	def set_sourcing_month(self):
		if not self.arn_no:
			return

		# Format: D25K22... where 25 is year, K is month (A=Jan, K=Nov)
		if len(self.arn_no) >= 4:
			try:
				year_code = self.arn_no[1:3]  # e.g., "25"
				month_code = self.arn_no[3].upper() # e.g., "K"

				month_map = {
					'A': 'Jan', 'B': 'Feb', 'C': 'Mar', 'D': 'Apr',
					'E': 'May', 'F': 'Jun', 'G': 'Jul', 'H': 'Aug',
					'I': 'Sep', 'J': 'Oct', 'K': 'Nov', 'L': 'Dec'
				}

				if month_code in month_map:
					month_name = month_map[month_code]
					self.sourcing_month = f"{month_name}-{year_code}"
			except Exception:
				pass

	def set_prime_month(self):
		if not self.decision_date:
			return

		try:
			date = getdate(self.decision_date)
			self.prime_month = date.strftime("%b-%y")
		except Exception:
				pass

	def set_po_type(self):
		if self.po_amount > 0:
			self.po_type = "Income"
		elif self.po_amount < 0:
			self.po_type = "Deduction"

	def set_employee_details(self):
		if not self.arn_no:
			return

		bpa_record = frappe.db.get_value(
			"BPA Records", 
			{"arn_no": self.arn_no}, 
			["employee_name", "employee_code"], 
			as_dict=True
		)

		if bpa_record:
			self.employee_name = bpa_record.employee_name
			self.employee_code = bpa_record.employee_code
		else:
			self.employee_name = "Unmapped"
			self.employee_code = "0"
