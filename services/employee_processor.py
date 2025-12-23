import time
import polars as pl
import warnings
warnings.filterwarnings("ignore", message="Could not determine dtype")

from bulk_upload.config import *
from datetime import datetime
from concurrent.futures import (
    ThreadPoolExecutor, 
    as_completed
)
from bulk_upload.db import (
    fetch_from_db,
    insert_into_db,
    update_job_progress,
    mark_job_completed,
    mark_job_failed
)
from bulk_upload.models import (
    MMTEmployeePayload,
    EmployeeOnboardingCreate,
    MMTSalaryAllocationPayload,
    SalutationTitleFR
)


def process_record(row) -> tuple[bool, list]:

    """Process a single record and insert into DB.
    Returns a list of failed records (empty if successful)."""

    # Preload Employees emp_id and emp_uuid mapping 
    # Should get refresh as new employee is added

    failed_records = []
        # print(json.dumps(row,indent=4))
        # continue

        # empId = row['emp_id']
        # designation = row['new_hierarchical_designation'].strip()
        # qualification = row['scale_considered'].strip().upper()
        # salary_slab = row['final_slab_considered'].strip().upper()

    # Mandatory fields check if those are null or not
    _error = []
    for field in FIELD_MANDATORY:
        if row.get(field) is None:
            _error.append(f"[FieldNotFound] : `{field}` ")

    record = {}
    for field, transform in FIELD_MAP.items():
        value = row.get(field)
        try:
            record[field] = transform(value)
        except Exception:
            record[field] = None

    print(record)

    empId = record['emp_id']
    designation = record['new_hierarchical_designation']
    qualification = record['scale_considered']
    salary_slab = record['final_slab_considered']

    print(f'qualification           : {qualification}')
    if qualification not in uniqueQualificationsInDB:
        qualification = QUALIFICATION_MAPP.get(qualification, qualification)
        print(f'qualification revised   : {qualification}')
    print(f'designation             : {designation}')

    if designation not in df_designationMapping.keys():
        designation = DESIGNATION_MAPP.get(designation,designation)
        print(f'designation revised     : {designation}')
    _designation_id = df_designationMapping.get(designation)
    if _designation_id is None: _error.append(f"[designationIdError] : Designation id not found corresponding to designation `{designation}` ")
    print(f'designation_id          : {_designation_id}')

    if record['sub_designation']:
        _sub_designation_id = df_subDesignationsMapping.get(record['sub_designation'], None)
    else:
        _sub_designation_id = None
    print(f'sub_designation_id      : {_sub_designation_id}')

    if not record['last_name']: record['last_name'] = ''
    if not record['remarks_salary_allocation']: record['remarks_salary_allocation'] = ''

    try:
        qualificationSlabId = (
                    df_qualification
                    .filter(
                        (pl.col("slab_name") == qualification) &
                        (pl.col("fk_designation_id") == _designation_id)
                    )
                    .select("id")
                    .to_series()[0]
        )
    except Exception as e:
        qualificationSlabId = None
        # row.update({'Error': f'Error in qualificationSlabID : {e}'})
        _error.append(f'[qualificationSlabIdError] : No id found corresponding to qualification `{qualification}` & designationId `{_designation_id}` -> {e}')
        # failed_records.append(row)
        # return failed_records
        # continue

    print(f"qualificationSlabId     : {qualificationSlabId}")

    print(f'salary_slab             : {salary_slab}')
    print(salary_slab in uniqueGradeSlabsInDB)
    
    try :
        workExSlabId = (
                    df_workExSlab
                    .filter(
                        (pl.col("grade") == salary_slab) &
                        (pl.col("fk_designation_id") == _designation_id) &
                        (pl.col("fk_qualification_slab") == qualificationSlabId)
                    )
                    .select("id")
                    .to_series()[0]
        )
    except Exception as e:
        workExSlabId = None
        # row.update({'Error': f'Error in workExSlabID : {e}'})
        _error.append(f'[workExSlabIdError] : No id found corresponding to grade `{salary_slab}`,fk_designation_id `{_designation_id}` & fk_qualification_slab `{qualificationSlabId}` -> {e}')
        # failed_records.append(row)
        # return failed_records
        # continue

    print(f"workExSlabId            : {workExSlabId}")

    # if record['national_head_emp_id'] is None: 
    #     row.update({'Error': f'nationalHeadEmpId not provided'})
    #     failed_records.append(row)
    #     return failed_records
    #     # continue
    try:
        nationalHeadEmpId = (
                    df_employees
                    .filter(
                        pl.col("emp_uuid") == str(record['national_head_emp_id'])
                    )
                    .select("emp_id")
                    .to_series()[0]
        )
        print(nationalHeadEmpId) 
    except Exception as e:
        nationalHeadEmpId = None
        # row.update({'Error': f'Error in nationalHeadEmpId : {e}'})
        _error.append(f"[nationalHeadEmpIdError] : No id found corresponding to emp_id `{record['national_head_emp_id']}` -> {e}")
        # failed_records.append(row)
        # return failed_records
        # # continue
    print(f"nationalHeadEmpId       : {nationalHeadEmpId}")

    # if record['country_head_emp_id'] is None: 
    #     row.update({'Error': f'countryHeadEmpId not provided'})
    #     failed_records.append(row)
    #     return failed_records
    #     # continue
    try:
        countryHeadEmpId = (
                    df_employees
                    .filter(
                        pl.col("emp_uuid") == str(record['country_head_emp_id'])
                    )
                    .select("emp_id")
                    .to_series()[0]
        )
    except Exception as e:
        countryHeadEmpId = None
        # row.update({'Error': f'Error in countryHeadEmpId : {e}'})
        _error.append(f"[countryHeadEmpIdError] : No id found corresponding to emp_id `{record['country_head_emp_id']}` -> {e}")
        # failed_records.append(row)
        # return failed_records
        # # continue
    print(f"countryHeadEmpId        : {countryHeadEmpId}\n")
    
    # return []

    # First CheckPoint of errors for a record
    if _error:
        row.update({'Error': ';\n'.join(_error)})
        failed_records.append(row)
        return False, failed_records
    # 1/0
    # --------------------- DB INSERTIONS ----------------------
    
    # --------- mmt_employees ---------
    payload_mmt_Employees = {
        
            # ================== MANDATORY FIELDS ==================

            # ---------- Identity ----------
            'emp_uuid': empId,                     # varchar(255) – generate if needed
            'email': record['email'],                        # varchar(255)
            'mobile_no': record['mobile_no'],                   # varchar(20)
            'country_iso': COUNTRY_ISO,                 # varchar(10)

            # ---------- Name ----------
            'first_name': record['first_name'],                  # NOT NULL
            'last_name': record['last_name'],                   # NOT NULL

            # ---------- Personal ----------
            'gender': record['gender'],                    # enum('male','female','other')
            'dob': record['date_of_birth'],                # stored as varchar (date string)
            'age': record['age'],                         # varchar

            # ---------- Family / Relations ----------
            'rel_person_name': REL_PERSON_NAME,             # NOT NULL in schema

            # ---------- Auth / Security ----------
            'auth_sign': AUTH_SIGN,                   # varchar(255)

            # ================== OPTIONAL FIELDS ==================

            # ---------- Name & Display ----------
            'title': record['title'],                       # enum('ms','mr','miss','mrs','ms')
            'middle_name': record['middle_name'] if record['middle_name'] else None,
            'display_name': f"{record['first_name'].strip()[0].upper()}.{record['middle_name'].strip()[0].upper()+'.' if record['middle_name'] and record['middle_name'].strip() else ''} {record['last_name'].strip().title()}", # usually first + last
            'salutation_title_fr': SalutationTitleFR.shri,       # enum('shri','late')

            # ---------- Personal ----------
            'is_married': IS_MARRIED,                     # tinyint(4)

            # ---------- Verification ----------
            'is_email_verified': IS_EMAIL_VARIFIED,              # tinyint(4)
            'is_mobile_verified': IS_MOBILE_VARIFIED,             # tinyint(4)

            # ---------- Status ----------
            'status': EMPLOYEE_STATUS,                  # enum('active','disable')
            'primary_relation': PRIMARY_RELATION,           # enum('d_o','w_o','s_o')

            # ---------- Media ----------
            'avatar_symbol': AVATAR_SYMBOL,               # text

            # ---------- Audit (optional but allowed) ----------
            'created_by': CREATED_BY_NONE_DEFAULT,
            'modified_by': MODIFIED_BY_ZERO_DEFAULT
    }

    # ---------------------------------
    # ----------------------------------------------------------

    try:
        record_mmt_employees = MMTEmployeePayload(**payload_mmt_Employees)
    except Exception as e:
        print(f"Error in MMTEmployeePayload creation: {e}")
        # row.update({'Error': f"Error in MMTEmployeePayload creation: {e}"})
        _error.append(f'[ValidationError] : Error in MMTEmployeePayload creation : {e} ')
        # failed_records.append(row)
        # return failed_records
        # # continue

    db_record_mmt_employees = record_mmt_employees.model_dump(exclude_none=True)
    print(db_record_mmt_employees)
    
    try:
        emp_id_DB = insert_into_db(
                    pl.DataFrame([db_record_mmt_employees]),
                    EMPLOYEES_PERSONAL_DETAILS,
                    *DB_CRED,
                )
        print(f"✅ Inserted employee ID: {emp_id_DB}")
    except Exception as e:
        print(f"Insertion into `{EMPLOYEES_PERSONAL_DETAILS}` failed: {e}")
        # row.update({'Error': f"Insertion into `{EMPLOYEES_PERSONAL_DETAILS}` failed: {e}"})
        _error.append(f'[DBInsertionError] : Insertion into `{EMPLOYEES_PERSONAL_DETAILS}` failed : {e} ')
        # failed_records.append(row)
        # return failed_records
        # # continue

    # --- emp_onboarded_companyinfo ---

    payload_emp_onboarded_companyinfo = {
            'depart_type': DEPARTMENT_TYPE_DEFAULT,                     # enum
            'month_of_joining': (datetime.strptime(record['DOJ'], "%d-%b-%y") if "-" in record['DOJ'] and record['DOJ'][2].isalpha() else datetime.strptime(record['DOJ'], "%Y-%m-%d")).strftime("%B"),
            'date_of_joining': record['DOJ'],
            'is_weekend_off': IS_WEEKEND_OFF_DEFAULT,               # 'yes' / 'no'
            'working_week_days_allocated': WORKING_WEEK_DAYS_ALLOCATED_DEFAULT,  # default = 5 if omitted
            'fiscal_year': FISCAL_YEAR_DEFAULT,
            'daily_wage': DAILY_WAGE_DEFAULT,
            'is_permanent': IS_PERMANENT_DEFAULT,                   # tinyint(1)
            'employeebility_type': EMPLOYEEBILITY_TYPE_DEFAULT,     # enum
            'company_working_days': COMPANY_WORKING_DAYS_DEFAULT,   # default = 30 if omitted
            'eligible_perks': ELIGIBLE_PERKS_DEFAULT,               # default = 1 if omitted
            'basic_salary': BASIC_SALARY_DEFAULT,
            'mapped_salary_default_fields': MAPPED_SALARY_DEFAULT_FIELDS_DEFAULT,

            'created_by': CREATED_BY_NONE_DEFAULT,
            'modified_by': MODIFIED_BY_NONE_DEFAULT,

            'fk_emp_id': emp_id_DB,
            'fk_region_id': df_regionsMapping.get(record['region']) if record['region'] else None,
            'fk_location_id': df_locationsMapping.get(record['location']) if record['location'] else None,
            'fk_branch_id': df_branchMapping.get(record['branch']) if record['branch'] else None,
            'fk_department_id': df_deptMapping.get(record['department']) if record['department'] else None,
            'fk_promoted_design_id': PROMOTED_DESIGNATION_ID_DEFAULT,
            'fk_current_design_id': _designation_id,
            'fk_country_head_emp': countryHeadEmpId,
            'fk_national_head_emp': nationalHeadEmpId,
            'fk_functional_role_id': df_funcRolesMapping.get(record['new_functional_role']) if record['new_functional_role'] else None,
            'fk_zone_id': df_zonesMapping.get(record['zone']) if record['zone'] else None,
            'fk_incentive_role_id': workExSlabId,

            'is_trainee': record['is_trainee'],                       # default = 0 if omitted
            'remarks': record['remarks_onboarding'],
            'is_additional_sa': record['is_additional_sa'],           # default = 0
            'is_super_annuation': record['is_super_annuation'],       # default = 0
            'annual_bonus': record['annual_bonus'],
            'adhoc_allowance': record['adhoc_allowance'],             # default = 0
            'adhoc_type': record['adhoc_type']                        # enum('Default','Manual')
            }
    
    # ---------------------------------
    # ---- mmt_salary_allocations -----

    payload_mmt_salary_allocations = {

            # ================== MANDATORY FIELDS ==================

            # ---------- Business ----------
            'remarks': record['remarks_salary_allocation'] if record['remarks_salary_allocation'] else '',   # varchar(255)
            
            # ================== OPTIONAL FIELDS ==================

            # ---------- Foreign Keys ----------
            'fk_emp_id': emp_id_DB,                    # bigint – employee id
            'fk_qualification_slab_id': qualificationSlabId,     # bigint
            'work_ex_slab_id': workExSlabId,              # bigint
            'fk_sub_designation': _sub_designation_id,           # bigint

            # ---------- Audit ----------
            'created_by': CREATED_BY_NONE_DEFAULT,
            'modified_by': MODIFIED_BY_NONE_DEFAULT
        }

    # ---------------------------------

    # Validations before DB Insertions
    try:
        record_emp_onboarded_companyinfo = EmployeeOnboardingCreate(**payload_emp_onboarded_companyinfo)
    except Exception as e:
        print(f"mmt_employee updated but Validation failed for {ONBOARDED_COMPANYINFO}: {e}")
        # row.update({'Error': f"mmt_employee updated but Validation failed for {ONBOARDED_COMPANYINFO}: {e}"})
        _error.append(f'[ValidationError] : mmt_employee updated but Validation failed for {ONBOARDED_COMPANYINFO} : {e} ')
        # failed_records.append(row)
        # return failed_records
        # # continue

    try:
        record_mmt_salary_allocations = MMTSalaryAllocationPayload(**payload_mmt_salary_allocations)
    except Exception as e:
        print(f"mmt_employee updated but Validation failed for {EMPLOYEES_SALARY_ALLOCATIONS}: {e}")
        # row.update({'Error': f"mmt_employee updated but Validation failed for {EMPLOYEES_SALARY_ALLOCATIONS}: {e}"})
        _error.append(f'[ValidationError] : mmt_employee updated but Validation failed for {EMPLOYEES_SALARY_ALLOCATIONS} : {e} ')
        # failed_records.append(row)
        # return failed_records
        # # continue

    # Insertion into emp_onboarded_companyinfo

    db_record_emp_onboarded_companyinfo = record_emp_onboarded_companyinfo.model_dump(exclude_none=True)
    print(db_record_emp_onboarded_companyinfo)

    try:
        emp_onboarded_id_DB = insert_into_db(
                    pl.DataFrame([db_record_emp_onboarded_companyinfo]),
                    ONBOARDED_COMPANYINFO,
                    *DB_CRED,
                )
        print(f"✅ Inserted employee onboarded company info ID: {emp_onboarded_id_DB}")
    except Exception as e:
        print(f"Insertion into {ONBOARDED_COMPANYINFO} failed: {e}")
        # row.update({'Error': f"Insertion into {ONBOARDED_COMPANYINFO} failed: {e}"})
        _error.append(f'[DBInsertionError] : Insertion into {ONBOARDED_COMPANYINFO} failed : {e} ')
        # failed_records.append(row)
        # return failed_records
        # # continue

    # Insertion into mmt_salary_allocations

    
    db_record_mmt_salary_allocations = record_mmt_salary_allocations.model_dump(exclude_none=True)
    print(db_record_mmt_salary_allocations)
    
    try:
        emp_salary_allo_id = insert_into_db(
                    pl.DataFrame([db_record_mmt_salary_allocations]),
                    EMPLOYEES_SALARY_ALLOCATIONS,
                    *DB_CRED,
        )
        print(f"✅ Inserted employee salary allocation ID: {emp_salary_allo_id}")
    except Exception as e:
        print(f"Insertion into `{EMPLOYEES_SALARY_ALLOCATIONS}` failed: {e}")
        # row.update({'Error': f"Insertion into `{EMPLOYEES_SALARY_ALLOCATIONS}` failed: {e}"})
        _error.append(f'[DBInsertionError] : Insertion into `{EMPLOYEES_SALARY_ALLOCATIONS}` failed : {e} ')
        # failed_records.append(row)
        # return failed_records
        # # continue
    
    # Error Check Point 2
    if _error:
        row.update({'Error': _error})
        failed_records.append(row)
        return False, failed_records 

    return True, failed_records


def process_excel(file_path: str, job_id: str) -> tuple[bool, pl.DataFrame]:
    """
    Main callable function for FastAPI
    Returns: (success: bool, failed_df: pl.DataFrame | None)
    """

    # ------------------------------------------------------------
    # --------------------- PRELOAD DB DATA ----------------------
    # ------------------------------------------------------------

    # Sub-Designations
    df_subDesignations = fetch_from_db(
        SUB_DESIGNATIONS,
        *DB_CRED
    )
    df_subDesignations = df_subDesignations.select([
        'id',
        'name'
    ])
    # uniqueSubDesignations = list(df_subDesignations['name'].unique())
    global df_subDesignationsMapping
    df_subDesignationsMapping = dict(zip(
        df_subDesignations['name'].to_list(),
        df_subDesignations['id'].to_list()
    ))

    # Regions
    df_regions = fetch_from_db(
        REGION,
        *DB_CRED
    )
    df_regions = df_regions.select([
        'id',
        'rg_name'
    ])
    uniqueRegions = list(df_regions['rg_name'].unique())
    # print(uniqueRegions)
    global df_regionsMapping
    df_regionsMapping = dict(zip(
        df_regions['rg_name'].to_list(),
        df_regions['id'].to_list()
    ))

    # Branches
    df_branch = fetch_from_db(
        BRANCH,
        *DB_CRED
    )
    df_branch = df_branch.select([
        'branch_id',
        'branch_name'
    ])
    uniqueBranches = list(df_branch['branch_name'].unique())
    # print(uniqueBranches)
    global df_branchMapping
    df_branchMapping = dict(zip(
        df_branch['branch_name'].to_list(),
        df_branch['branch_id'].to_list()
    ))

    # Locations
    df_locations = fetch_from_db(
        LOCATION,
        *DB_CRED
    )
    df_locations = df_locations.select([
        'location_id',
        'location_name'
    ])
    uniqueLocations = list(df_locations['location_name'].unique())
    # print(uniqueLocations)
    global df_locationsMapping
    df_locationsMapping = dict(zip(
        df_locations['location_name'].to_list(),
        df_locations['location_id'].to_list()
    ))

    # Zones
    df_zones = fetch_from_db(
        ZONE_MASTER,
        *DB_CRED
    )
    df_zones = df_zones.select([
        'id',
        'rg_name'
    ])
    uniqueZones = list(df_zones['rg_name'].unique())
    # print(uniqueZones)
    global df_zonesMapping
    df_zonesMapping = dict(zip(
        df_zones['rg_name'].to_list(),
        df_zones['id'].to_list()
    ))

    # Departments
    df_departments = fetch_from_db(
        DEPARTMENT,
        *DB_CRED
    )
    df_departments = df_departments.select([
        'dept_id',
        'dept_name'
    ])  
    uniqueDepartments = list(df_departments['dept_name'].unique())
    # print(uniqueDepartments)
    global df_deptMapping
    df_deptMapping = dict(zip(
        df_departments['dept_name'].to_list(),
        df_departments['dept_id'].to_list()
    ))

    # Functional Roles
    global df_functionalRoles
    df_functionalRoles = fetch_from_db(
        FUNCTIONAL_ROLES,
        *DB_CRED   
    )
    df_functionalRoles = df_functionalRoles.select([ 
        'id',
        'role_name'
    ])
    uniqueFunctionalRoles = list(df_functionalRoles['role_name'].unique())
    # print(uniqueFunctionalRoles)
    global df_funcRolesMapping
    df_funcRolesMapping = dict(zip(
        df_functionalRoles['role_name'].to_list(),
        df_functionalRoles['id'].to_list()
    ))

    # Preload Qualification Slabs and WorkEx Slabs
    global df_workExSlab
    df_workExSlab = fetch_from_db(
        SLAB_MASTER,
        *DB_CRED
    )

    global df_qualification
    df_qualification = fetch_from_db(
        QUALIFICATION,
        *DB_CRED
    )
    df_qualification = df_qualification.select(
        "id", "slab_name", "fk_designation_id"
    )
    df_qualification = df_qualification.with_columns(
        pl.col("slab_name")
        .cast(pl.Utf8)
        .str.replace_all(r"^\s+|\s+$", "")   # strip using regex
        # .str.to_uppercase()
    )

    # Designations
    df_designation = fetch_from_db(
        DG_DESIGNATIONS,
        *DB_CRED
    )
    df_designation = df_designation['design_id','designation_name']
    global df_designationMapping
    df_designationMapping = dict(zip(
        df_designation['designation_name'].to_list(),
        df_designation['design_id'].to_list()
    ))

    # Employee Table
    global df_employees
    df_employees = fetch_from_db(
        EMPLOYEES_PERSONAL_DETAILS,
        *DB_CRED
    ).select(['emp_id', 'emp_uuid'])


    # df = pl.read_excel(file_path, sheet_name=None)
    df = pl.read_excel(file_path)

    uniqueQualificationsInFile = list(set(df['scale_considered'].unique().to_list()))
    uniqueQualificationsInFile = [i.strip().upper() for i in uniqueQualificationsInFile]
    uniqueQualificationsInFile.sort()
    # print(uniqueQualificationsInFile)

    global uniqueQualificationsInDB
    uniqueQualificationsInDB = df_qualification['slab_name'].unique().to_list()
    uniqueQualificationsInDB.sort()
    # print(uniqueQualificationsInDB)

    global uniqueGradeSlabsInDB
    uniqueGradeSlabsInDB = df_workExSlab['grade'].unique().to_list()
    uniqueGradeSlabsInDB.sort()
    # print(uniqueGradeSlabsInDB)
    # 1/0
    # ---------------- PROCESS RECORDS ----------------
    all_failed_records = []
    BATCH_UPDATE_SIZE = 10  
    processed = 0
    # failed_batch_count = 0
    start = time.time()

    print(f'[Info] Using {BATCH_WORKERS} workers.')

    try:
        with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as executor:
            futures = [
                executor.submit(process_record, row)
                for row in df.iter_rows(named=True)
            ]

            for future in as_completed(futures):
                success, failed = future.result()

                processed += 1

                if not success:
                    all_failed_records.extend(failed)

                # 🔹 batch DB update
                if processed % BATCH_UPDATE_SIZE == 0:
                    update_job_progress(
                        job_id,
                        BATCH_UPDATE_SIZE,
                        *DB_CRED
                    )

        # ✅ FLUSH remaining records ONCE (outside loop)
        remaining = processed % BATCH_UPDATE_SIZE
        if remaining:
            update_job_progress(
                job_id,
                remaining,
                *DB_CRED
            )

        # ✅ Mark job completed ONCE
        mark_job_completed(job_id, *DB_CRED)

    except Exception as e:
        # ❗ VERY IMPORTANT
        mark_job_failed(job_id, *DB_CRED)
        raise   # re-raise so API knows something went wrong

    end = time.time()


    print(f"Time taken: {end - start:.2f}s")
    print("Total Records Failed:", len(all_failed_records))

    if not all_failed_records:
        return True, None

    failed_df = pl.DataFrame(all_failed_records)
    return False, failed_df


if __name__ == "__main__":
        # File Paths
    print("ok1")
    FAILED_RECORDS = 'bulk_upload\failed_records.xlsx'
    RECORDS = 'bulk_upload\Digikit_bulk_main_data_1.xlsx'
    success, failed = process_excel(RECORDS)

    failed.write_excel(FAILED_RECORDS)
    print(success)
    print(failed)
    # main()