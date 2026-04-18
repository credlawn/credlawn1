import frappe
from frappe import _
import requests
from datetime import datetime, timedelta
import pytz
from frappe.utils import now_datetime, getdate, add_months, flt

@frappe.whitelist()
def execute_sync():
    """Main production entry point for Attendance Sync - Enqueues background job with Lock"""
    lock_key = "attendance_sync_lock"
    if frappe.cache().get(lock_key):
        return _("An Attendance Sync job is already running in the background. Please wait.")
    
    frappe.cache().set_value(lock_key, 1, expires_in_sec=7200)

    try:
        frappe.enqueue(
            method='credlawn.credlawn.doctype.attendance.sync_atn.sync_job',
            queue='long',
            timeout=7200,
            is_async=True,
            job_name='attendance_sync'
        )
        return _("Attendance Sync & Mapping initiated. Check logs for progress.")
    except Exception as e:
        frappe.cache().delete(lock_key)
        frappe.log_error(frappe.get_traceback(), "Attendance Sync Enqueue Error")
        return _("Failed to start sync: {0}").format(str(e))

def sync_job():
    """Master Sync Job: Enrich existing and Fill gaps"""
    lock_key = "attendance_sync_lock"
    try:
        pb_url = frappe.conf.get("pocketbase_url")
        pb_token = frappe.conf.get("pocketbase_auth_token")

        if not pb_url or not pb_token:
            return

        # 1. Fetch User Master Data (Global Map)
        emp_data_map = {}
        u_page, u_per_page = 1, 500
        while True:
            users = fetch_pocketbase_records(pb_url, pb_token, "users", page=u_page, per_page=u_per_page, pb_filter='no_atn=false')
            if not users: break
            for u in users:
                code = str(u.get('employee_code') or "").strip()
                if not code: continue
                p_start = convert_to_ist(u.get('payroll_start_date'))
                l_work = convert_to_ist(u.get('last_working_date'))
                emp_data_map[code] = {
                    "name": u.get('employee_name') or u.get('name') or u.get('username'),
                    "salary": flt(u.get('salary')),
                    "start": p_start.date() if p_start else None,
                    "last": l_work.date() if l_work else None
                }
            if len(users) < u_per_page: break
            u_page += 1

        # 2. Sync Existing Records from PB Attendance
        existing_records = {
            d.pb_id: d.pb_updated for d in frappe.get_all("Attendance", 
            filters={"pb_id": ["is", "set"]}, 
            fields=["pb_id", "pb_updated"])
        }

        page, per_page = 1, 500
        initial_res = fetch_pocketbase_records(pb_url, pb_token, "attendance", page=1, per_page=1, return_raw=True)
        total_items = initial_res.get('totalItems', 0) if initial_res else 0

        total_fetched = 0
        while True:
            records = fetch_pocketbase_records(pb_url, pb_token, "attendance", page=page, per_page=per_page)
            if not records: break
            for record in records:
                try:
                    pb_id, pb_updated = record.get('id'), record.get('updated')
                    if not pb_id: continue
                    doc = None
                    if pb_id in existing_records:
                        doc_name = frappe.db.get_value("Attendance", {"pb_id": pb_id}, "name")
                        doc = frappe.get_doc('Attendance', doc_name)
                        if existing_records[pb_id] == pb_updated and doc.monthly_salary:
                            continue
                    else:
                        doc = frappe.new_doc('Attendance')
                    
                    doc.flags.from_pb_sync = True
                    update_doc_fields(doc, record, emp_data_map)
                    doc.save(ignore_permissions=True)
                except: continue

            total_fetched += len(records)
            publish_progress(int((total_fetched / total_items) * 50) if total_items else 25, "Syncing Logs...")
            if len(records) < per_page: break
            page += 1
            frappe.db.commit()

        # 3. Gap Filling (Only missing days up to Today)
        fill_attendance_gaps(pb_url, pb_token, emp_data_map)
        publish_progress(100, "Full Sync Completed.")

    except Exception as e:
        frappe.log_error(frappe.get_traceback(), "Attendance Job Error")
    finally:
        frappe.cache().delete(lock_key)

def fill_attendance_gaps(pb_url, pb_token, emp_data_map):
    try:
        # Fetch Holidays from PB
        holidays_raw = fetch_pocketbase_records(pb_url, pb_token, "holiday", per_page=1000, pb_filter='active=true')
        holiday_dates = set([str(convert_to_ist(h.get('holiday_date')).date()) for h in holidays_raw if h.get('holiday_date')])

        # Map Existing Attendance (employee + date)
        existing_raw = frappe.db.sql("SELECT employee_code, attendance_date FROM `tabAttendance`", as_dict=True)
        existing_map = set([(str(d.employee_code).strip(), str(d.attendance_date)) for d in existing_raw])

        today = getdate()
        new_batch = []
        
        for emp_code, info in emp_data_map.items():
            check_date = info['start']
            if not check_date: continue
            
            # Hard boundary: Don't go past Today or Last working date
            end_boundary = today
            if info['last']:
                end_boundary = min(today, info['last'])
            
            # THE LOOP: Iterate Day by Day
            while check_date <= end_boundary:
                date_str = str(check_date)
                
                if (emp_code, date_str) not in existing_map:
                    status = "Holiday" if date_str in holiday_dates else "Absent"
                    m_salary = flt(info['salary'])
                    d_salary = m_salary / 30.0
                    mult = 1.0 if status == "Holiday" else 0.0
                    
                    att_month = check_date.strftime("%b-%y")
                    payroll_dt = add_months(check_date, 1) if check_date.day > 20 else check_date
                    p_month = payroll_dt.strftime("%b-%y")

                    new_batch.append({
                        "name": frappe.generate_hash(length=12),
                        "employee_name": info['name'],
                        "employee_code": emp_code,
                        "attendance_date": date_str,
                        "approved_status": status,
                        "monthly_salary": m_salary,
                        "daily_salary": d_salary,
                        "payable_amount": round(d_salary * mult, 2),
                        "payroll_start_date": str(info['start']),
                        "last_working_date": str(info['last']) if info['last'] else None,
                        "attendance_month": att_month,
                        "payroll_month": p_month,
                        "creation": now_datetime(), "modified": now_datetime(), "owner": "Administrator", "modified_by": "Administrator"
                    })

                # CRITICAL: Increment the pointer!
                check_date += timedelta(days=1)
                
                # Internal Batching
                if len(new_batch) >= 1000:
                    bulk_insert_gap_records(new_batch)
                    new_batch = []

        if new_batch:
            bulk_insert_gap_records(new_batch)

    except Exception:
        frappe.log_error(frappe.get_traceback(), "Gap Filler Fatal Error")

def update_doc_fields(doc, pb_record, emp_data_map):
    doc.approved_status = pb_record.get('status') or "Pending"
    cin = convert_to_ist(pb_record.get('check_in_time'))
    if cin: doc.attendance_date = cin.date(); doc.check_in_time = cin.strftime("%H:%M:%S")
    cout = convert_to_ist(pb_record.get('check_out_time'))
    if cout: doc.check_out_time = cout.strftime("%H:%M:%S")

    emp_code = str(pb_record.get('employee_code') or "").strip()
    doc.employee_code = emp_code
    doc.pb_id, doc.pb_created, doc.pb_updated = pb_record.get('id'), pb_record.get('created'), pb_record.get('updated')

    if emp_code in emp_data_map:
        info = emp_data_map[emp_code]
        doc.employee_name = info['name']
        doc.monthly_salary = info['salary']
        doc.payroll_start_date = info['start']
        doc.last_working_date = info['last']
    return True

def bulk_insert_gap_records(records):
    if not records: return
    fields = ["name", "employee_name", "employee_code", "attendance_date", "approved_status", "attendance_month", "payroll_month", "monthly_salary", "daily_salary", "payable_amount", "payroll_start_date", "last_working_date", "creation", "modified", "owner", "modified_by"]
    query = f"INSERT INTO `tabAttendance` ({', '.join(['`' + f + '`' for f in fields])}) VALUES " + ", ".join(["(" + ", ".join(["%s"] * len(fields)) + ")"] * len(records))
    frappe.db.sql(query, [r.get(f) for r in records for f in fields])
    frappe.db.commit()

def fetch_pocketbase_records(pb_url, pb_token, collection, page=1, per_page=500, pb_filter="", return_raw=False):
    try:
        api_url = f"{pb_url.rstrip('/')}/api/collections/{collection}/records"
        params = {"perPage": per_page, "page": page, "filter": f"({pb_filter})" if pb_filter else ""}
        res = requests.get(api_url, headers={"Authorization": f"Bearer {pb_token}"}, params=params, timeout=30)
        if res.status_code == 200:
            data = res.json()
            return data if return_raw else data.get('items', [])
        return {} if return_raw else []
    except: return {} if return_raw else []

def convert_to_ist(utc_str):
    if not utc_str: return None
    try:
        dt = datetime.fromisoformat(str(utc_str).replace(' ', 'T').replace('Z', ''))
        return dt.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('Asia/Kolkata')).replace(tzinfo=None)
    except: return None

def publish_progress(percentage, message, failed=False):
    frappe.publish_realtime("attendance_sync_progress", {"percentage": percentage, "message": message, "failed": failed})
