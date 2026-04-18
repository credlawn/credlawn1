import frappe
from frappe.model.document import Document
from frappe.utils import getdate, add_months, flt

class Attendance(Document):
    def validate(self):
        """Auto-calculate Attendance months and Salary Payroll amounts."""
        # 1. Month Calculations
        if self.attendance_date:
            dt = getdate(self.attendance_date)
            self.attendance_month = dt.strftime("%b-%y")
            
            # Payroll cycle: 21st to 20th
            payroll_dt = add_months(dt, 1) if dt.day > 20 else dt
            self.payroll_month = payroll_dt.strftime("%b-%y")

        # 2. Salary Calculation (The 'Pro' Way)
        self.calculate_payroll_amounts()

    def calculate_payroll_amounts(self):
        """Calculates daily salary impact based on status multiplier."""
        if not self.monthly_salary:
            self.daily_salary = 0
            self.payable_amount = 0
            return

        # Standard daily rate (base 30)
        self.daily_salary = flt(self.monthly_salary) / 30.0

        # Status Multipliers logic
        multipliers = {
            "Full-day": 1.0,
            "Present": 1.0,
            "Leave": 1.0,
            "Holiday": 1.0,
            "Half-day": 0.5,
            "Absent": 0.0,
            "Pending": 0.0
        }

        # Match multiplier based on approved_status
        factor = multipliers.get(self.approved_status, 0.0)
        
        # Calculate final rounded payable amount for this record
        self.payable_amount = round(self.daily_salary * factor, 2)

    def on_update(self):
        """Two-way sync with PocketBase."""
        if hasattr(self.flags, 'from_pb_sync') and self.flags.from_pb_sync:
            return
        
        if self.has_value_changed('approved_status') and self.pb_id:
            frappe.enqueue(
                'credlawn.credlawn.doctype.attendance.sync_atn.push_status_to_pb',
                doc_name=self.name,
                now=frappe.flags.in_test
            )
