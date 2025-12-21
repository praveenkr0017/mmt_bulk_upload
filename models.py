from enum import Enum
from pydantic import (
    BaseModel, 
    EmailStr, 
    ConfigDict
)
from typing import Optional
from decimal import Decimal

# mmt_emp_onboarded_companyinfo
class DepartType(str, Enum):
    technical = "technical"
    non_technical = "non technical"

class YesNo(str, Enum):
    yes = "yes"
    no = "no"

class EmploymentTypeForOnboarding(str, Enum):
    parttime = "parttime"
    fulltime = "fulltime"
    freelance = "freelance"
    contract = "contract"
    incentive = "incentive"

class AdhocType(str, Enum):
    default = "Default"
    manual = "Manual"

class EmployeeOnboardingCreate(BaseModel):
    model_config = {
        "use_enum_values": True
    }
    depart_type: DepartType
    month_of_joining: str
    date_of_joining: str
    is_weekend_off: YesNo
    fiscal_year: str
    daily_wage: int
    is_permanent: int
    employeebility_type: EmploymentTypeForOnboarding
    basic_salary: int

    # Optional / defaulted by DB
    working_week_days_allocated: Optional[int] = 5
    company_working_days: Optional[int] = None
    eligible_perks: Optional[int] = None
    is_trainee: Optional[int] = None
    is_additional_sa: Optional[int] = None
    is_super_annuation: Optional[int] = None
    adhoc_allowance: Optional[int] = None
    adhoc_type: Optional[AdhocType] = None

    remarks: Optional[str] = None
    mapped_salary_default_fields: Optional[str] = None

    created_by: Optional[int] = None
    modified_by: Optional[int] = None

    fk_emp_id: Optional[int] = None
    fk_region_id: Optional[int] = None
    fk_location_id: Optional[int] = None
    fk_branch_id: Optional[int] = None
    fk_department_id: Optional[int] = None
    fk_national_head_emp: Optional[int] = None


# mmt_employees
class Title(str, Enum):
    ms = "ms"
    mr = "mr"
    miss = "miss"
    mrs = "mrs"

class SalutationTitleFR(str, Enum):
    shri = "shri"
    late = "late"

class Gender(str, Enum):
    male = "male"
    female = "female"
    other = "other"


class EmployeeStatus(str, Enum):
    active = "active"
    disable = "disable"

class PrimaryRelation(str, Enum):
    d_o = "d_o"   # Daughter of
    s_o = "s_o"   # Son of
    w_o = "w_o"   # Wife of


class MMTEmployeePayload(BaseModel):
    # ---------- Identity ----------
    emp_uuid: str
    email: EmailStr
    mobile_no: str
    country_iso: str

    # ---------- Name ----------
    title: Optional[Title] = None
    first_name: str
    middle_name: Optional[str] = None
    last_name: str
    display_name: str
    salutation_title_fr: Optional[SalutationTitleFR] = None

    # ---------- Personal ----------
    gender: Gender
    dob: str                          # stored as varchar in DB
    age: str                          # varchar
    is_married: int = 0               # tinyint(4)

    # ---------- Relations ----------
    rel_person_name: str

    # ---------- Auth ----------
    auth_sign: str

    # ---------- Verification ----------
    is_email_verified: int = 0
    is_mobile_verified: int = 0

    # ---------- Status ----------
    status: EmployeeStatus = None
    primary_relation: PrimaryRelation = None

    # ---------- Media ----------
    avatar_symbol: Optional[str] = None

    # ---------- Audit ----------
    created_by: Optional[int] = None
    modified_by: Optional[int] = None

    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid"
    )

# mmt_emp_qualifications
class CourseType(str, Enum):
    highschool = "highschool"
    intermediate = "intermediate"
    ug = "ug"
    pg = "pg"
    doctorate = "doctorate"
    diploma = "diploma"
    others = "others"
    certificates = "certificates"


class MMTEmployeeQualificationPayload(BaseModel):
    # ---------- Foreign Keys ----------
    fk_emp_id: Optional[int] = None
    fk_course_id: Optional[int] = None
    fk_stream_id: Optional[int] = None

    # ---------- Qualification ----------
    course_type: CourseType
    university_college: str
    start_year: str
    end_year: str

    # ---------- Scores ----------
    percentage: Optional[int] = None
    cgpa: Optional[int] = None

    # ---------- Address ----------
    country: Optional[str] = 'India'
    state: Optional[str] = None
    city_town_district: Optional[str] = None
    zip_code: Optional[str] = None
    street_address: Optional[str] = None
    landmark: Optional[str] = None

    # ---------- Audit ----------
    created_by: Optional[int] = None
    modified_by: Optional[int] = None

    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid"
    )

# mmt_emp_work_experiences
class EmploymentType(str, Enum):
    full_time = "Full-time"
    part_time = "Part-time"
    contract = "Contract"
    other = "Other"

class MMTEmployeeWorkExperiencePayload(BaseModel):
    # ---------- Foreign Key ----------
    fk_emp_id: Optional[int] = None

    # ---------- Organization ----------
    company_name: str
    job_title: str
    job_description: Optional[str] = None
    location: Optional[str] = None

    # ---------- Employment ----------
    employment_type: EmploymentType
    start_date: str                     # varchar in DB
    end_date: Optional[str] = None

    # ---------- Compensation ----------
    last_withdrawn_salary: Optional[Decimal] = None

    # ---------- Exit ----------
    reason_for_leaving: Optional[str] = None
    comments: Optional[str] = None

    # ---------- Audit ----------
    created_by: Optional[int] = None
    modified_by: Optional[int] = None

    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid"
    )

# mmt_salary_allocations
class MMTSalaryAllocationPayload(BaseModel):
    # ---------- Foreign Keys ----------
    fk_emp_id: Optional[int] = None
    fk_qualification_slab_id: Optional[int] = None
    work_ex_slab_id: Optional[int] = None
    fk_sub_designation: Optional[int] = None

    # ---------- Business ----------
    remarks: str

    # ---------- Audit ----------
    created_by: Optional[int] = None
    modified_by: Optional[int] = None

    model_config = ConfigDict(
        use_enum_values=True,
        extra="forbid"
    )