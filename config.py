import os 

from dotenv import load_dotenv
from bulk_upload.models import (
    EmployeeStatus, 
    PrimaryRelation, 
    YesNo, 
    EmploymentTypeForOnboarding, 
    DepartType
)

load_dotenv(override=True)

def clean_str(val):
    return val.strip() if isinstance(val, str) else val

def clean_upper(val):
    return val.strip().upper() if isinstance(val, str) else val

def clean_age(val):
    return str(int(val)) if isinstance(val, (int, float)) else val

# Database Credentials
DB_HOST=os.getenv("DB_HOST")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD=os.getenv("DB_PASSWORD")
DATABASE=os.getenv("DATABASE")
DB_PORT=os.getenv('DB_PORT')
DB_CRED = DB_HOST, DB_USER, DB_PASSWORD, DATABASE, int(DB_PORT)

# mmt_uploadprocess_logs updation
BATCH_UPDATE_SIZE = int(os.getenv("BATCH_UPDATE_SIZE"))

# Table Names
DG_DESIGNATIONS = 'dg_designations'
FUNCTIONAL_ROLES = 'mmt_functional_roles_master'
MMT_INCENTIVE_ROLES = 'mmt_incentive_roles'
DEPARTMENT = 'dg_departments'
ZONE_MASTER = 'mmt_zones_master'
REGION = 'dg_regions'
BRANCH = 'dg_branches'
LOCATION = 'dg_locations'
ONBOARDED_COMPANYINFO = 'emp_onboarded_companyinfo'
QUALIFICATION = 'mmt_qualification_slabs_master'
WORK_EX = 'mmt_work_ex_slabs_master'
SLAB_MASTER = 'mmt_work_ex_slabs_master'
COURSES_MASTER = 'mmt_courses'
STREAMS_MASTER = 'mmt_streams'
SUB_DESIGNATIONS = 'mmt_sub_designations'
EMPLOYEES_PERSONAL_DETAILS = 'mmt_employees'
EMPLOYEES_QUALIFICATIONS = 'mmt_emp_qualifications'
EMPLOYEES_WORKEX = 'mmt_emp_work_experiences'
EMPLOYEES_SALARY_ALLOCATIONS = 'mmt_salary_allocations'   
UPLOAD_PROCESS_LOGS = 'mmt_uploadprocess_logs'

# Mappings
DESIGNATION_MAPP = {
    'Assistant Engineer': 'Assistant',
    'Assistant Manager': 'Assistant Manager (AM)',
    'Associate Manager': 'Associate Manager (ASM)',
    'Asst. General Manager': 'Assistant General Manager (AGM)',
    'Deputy General Manager': 'Deputy General Manager (DGM)',
    'Deputy Manager': 'Deputy Manager (DM)',
    'Engineer': 'Assistant', # temporary
    'Executive': 'Executive',
    'Manager': 'Manager (MGR)',
    'Sr. Engineer': 'Assistant', # temporary
    'Sr. Executive': 'Senior Executive (SE)',
    'Sr. Manager': 'Senior Manager (SM)'
}

# Qualification Mapping
QUALIFICATION_MAPP = {
    'BE' : 'Engineering',
    'BE/MBA' : 'Engineering-MBA',
    'DIPLOMA/MBA' : 'Diploma-MBA',
    'GRADUATION/MBA' : 'Graduation-MBA',
    'POSTGRADUATE' : 'Graduation'
}

# Cleaning Functions
FIELD_MAP = {
    "email": clean_str,
    "mobile_no": str,
    "title": clean_str,
    "first_name": clean_str,
    "middle_name": clean_str,
    "last_name": clean_str,
    "gender": clean_str,
    "is_married": clean_str,
    "date_of_birth": str,
    "age": clean_age,
    "emp_id": str,
    "DOJ": str,
    "new_hierarchical_designation": clean_str,
    "new_functional_role": clean_str,
    "role_for_ipp_calculation": clean_str,
    "department_ind_performance_pay": clean_str,
    "department": clean_str,
    "region": clean_str,
    "branch": clean_str,
    "location": clean_str,
    "zone": str,
    "year_of_passing": clean_str,
    "scale_considered": clean_str,
    "final_slab_considered": clean_upper,
    "is_trainee": clean_str,
    "is_additional_sa": clean_str,
    "is_super_annuation": clean_str,
    "annual_bonus": float,
    "adhoc_allowance": float,
    "adhoc_type": clean_str,
    "remarks_onboarding": clean_str,
    "remarks_salary_allocation": clean_str,
    "sub_designation": clean_str,
    "national_head_emp_name": clean_str,
    "national_head_emp_id": clean_str,
    "country_head_emp_name": clean_str,
    "country_head_emp_id": clean_str
}

# Mandatory Fields
FIELD_MANDATORY = [
    "email",
    "mobile_no",
    "title",
    "first_name",
    # "middle_name",
    # "last_name",
    "gender",
    "is_married",
    "date_of_birth",
    "age",
    "emp_id",
    "DOJ",
    "new_hierarchical_designation",
    "new_functional_role",
    "role_for_ipp_calculation",
    "department_ind_performance_pay",
    "department",
    "region",
    "branch",
    "location",
    "zone",
    "year_of_passing",
    "scale_considered",
    "final_slab_considered",
    "is_trainee",
    "is_additional_sa",
    "is_super_annuation",
    # "annual_bonus",
    # "adhoc_allowance",
    "adhoc_type",
    # "remarks_onboarding",
    # "remarks_salary_allocation",
    "sub_designation",
    "national_head_emp_name",
    "national_head_emp_id",
    "country_head_emp_name",
    "country_head_emp_id"
]

# Default Values
COUNTRY_ISO = 'India'
REL_PERSON_NAME = ''
AUTH_SIGN = ''
IS_MARRIED = 0
IS_EMAIL_VARIFIED = 1
IS_MOBILE_VARIFIED = 1
EMPLOYEE_STATUS = EmployeeStatus.active
PRIMARY_RELATION = PrimaryRelation.s_o
AVATAR_SYMBOL = ''
CREATED_BY_NONE_DEFAULT = None
CREATED_BY_ZERO_DEFAULT = 0
MODIFIED_BY_ZERO_DEFAULT = 0
MODIFIED_BY_NONE_DEFAULT = None
IS_WEEKEND_OFF_DEFAULT = YesNo.yes
FISCAL_YEAR_DEFAULT = YesNo.yes
DAILY_WAGE_DEFAULT = 0
IS_PERMANENT_DEFAULT = '1'
EMPLOYEEBILITY_TYPE_DEFAULT = EmploymentTypeForOnboarding.fulltime
COMPANY_WORKING_DAYS_DEFAULT = 30
ELIGIBLE_PERKS_DEFAULT = None
BASIC_SALARY_DEFAULT = 0
WORKING_WEEK_DAYS_ALLOCATED_DEFAULT = None
MAPPED_SALARY_DEFAULT_FIELDS_DEFAULT = None
DEPARTMENT_TYPE_DEFAULT = DepartType.technical
PROMOTED_DESIGNATION_ID_DEFAULT = None

# Batch Settings
BATCH_WORKERS = 5



