import dlt
from pyspark.sql.functions import *

#----------------------------------------------------------------------------------------------------------------------
# Single‑rule decorators
# @dlt.expect(name, condition) → keep invalid rows and record metrics (default “warn/retain” behavior).
# @dlt.expect_or_drop(name, condition) → drop rows that violate the rule before writing. Metrics reflect how many were dropped.
# @dlt.expect_or_fail(name, condition) → fail the update immediately if any row violates the rule (only the affected flow fails; parallel flows continue).


# Multi‑rule (group) decorators
# @dlt.expect_all({name: condition, ...}) → apply many rules; keep invalid rows; collect per‑rule metrics.
# @dlt.expect_all_or_drop({name: condition, ...}) → apply many rules; drop rows that violate any rule.
# @dlt.expect_all_or_fail({name: condition, ...}) → apply many rules; fail the update if any rule is violated.
#----------------------------------------------------------------------------------------------------------------------

@dlt.table(
    name="employees_silver",
    comment="Cleansed employees data, tracking data quality with a view. We ensude valid JSON, id, first_name and last_name",
)
@dlt.expect_or_drop("no_rescued_data", "_rescued_data is null")
@dlt.expect_or_drop("valid_id", "employee_id is not null")
@dlt.expect_or_drop("valid_first_name", "firstname is not null and length(firstname) > 0")
@dlt.expect_or_drop("valid_last_name", "lastname is not null and length(lastname) > 0")
# Above seperate contrainsts can also be wriiten like this in using muti-rule decorator->
#----------------------------------------------------------------------------
# @dlt.expect_all_or_drop({
#     "no_rescued_data": "_rescued_data is null",
#     "valid_id":  "employee_id is not null",
#     "valid_first_name":   "firstname is not null and length(firstname) > 0",
#     "valid_last_name":    "lastname is not null and length(lastname) > 0"
# })
#----------------------------------------------------------------------------
def employees_silver():
    return dlt.read_stream("employees_raw").select("employee_id", "firstname", "lastname", "_rescued_data")

@dlt.table(
    name="locations_silver",
    comment="Cleansed locations data, tracking data quality with a view. We ensude valid JSON, id, name, base_location, active_in_base_location, base_location_end_date",
)
@dlt.expect_or_drop("no_rescued_data", "_rescued_data is null")
@dlt.expect_or_drop("valid_id", "employee_id is not null")
@dlt.expect_or_drop("valid_employee_name", "employee_name is not null and length(employee_name) > 0")
@dlt.expect_or_drop("valid_base_location", "base_location is not null and length(base_location) > 0")
@dlt.expect_or_drop("valid_active_flag", "active_in_base_location in ('Y','N')")
# End date semantics: allowed to be NULL when active, required when inactive.
@dlt.expect(
    "end_date_only_when_inactive",
    "(active_in_base_location = 'N' and base_location_end_date is not null) "
    "or (active_in_base_location = 'Y' and base_location_end_date is null)"
)
def locations_silver():
    return dlt.read_stream("locations_raw").select("employee_id", "employee_name", "base_location", "active_in_base_location", "base_location_end_date", "_rescued_data")