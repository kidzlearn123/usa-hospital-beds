import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "database-usa-hospital-beds", table_name = "hospitalusa_hospital_beds_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "database-usa-hospital-beds", table_name = "hospitalusa_hospital_beds_csv", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("x", "double", "x", "double"), ("y", "double", "y", "double"), ("objectid", "long", "objectid", "long"), ("hospital_name", "string", "hospital_name", "string"), ("hospital_type", "string", "hospital_type", "string"), ("hq_address", "string", "hq_address", "string"), ("hq_address1", "string", "hq_address1", "string"), ("hq_city", "string", "hq_city", "string"), ("hq_state", "string", "hq_state", "string"), ("hq_zip_code", "long", "hq_zip_code", "long"), ("county_name", "string", "county_name", "string"), ("state_name", "string", "state_name", "string"), ("state_fips", "long", "state_fips", "long"), ("cnty_fips", "long", "cnty_fips", "long"), ("fips", "long", "fips", "long"), ("num_licensed_beds", "long", "num_licensed_beds", "long"), ("num_staffed_beds", "long", "num_staffed_beds", "long"), ("num_icu_beds", "long", "num_icu_beds", "long"), ("adult_icu_beds", "long", "adult_icu_beds", "long"), ("pedi_icu_beds", "long", "pedi_icu_beds", "long"), ("bed_utilization", "double", "bed_utilization", "double"), ("potential_increase_in_bed_capac", "long", "potential_increase_in_bed_capac", "long"), ("avg_ventilator_usage", "long", "avg_ventilator_usage", "long")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("x", "double", "x", "double"), ("y", "double", "y", "double"), ("objectid", "long", "objectid", "long"), ("hospital_name", "string", "hospital_name", "string"), ("hospital_type", "string", "hospital_type", "string"), ("hq_address", "string", "hq_address", "string"), ("hq_address1", "string", "hq_address1", "string"), ("hq_city", "string", "hq_city", "string"), ("hq_state", "string", "hq_state", "string"), ("hq_zip_code", "long", "hq_zip_code", "long"), ("county_name", "string", "county_name", "string"), ("state_name", "string", "state_name", "string"), ("state_fips", "long", "state_fips", "long"), ("cnty_fips", "long", "cnty_fips", "long"), ("fips", "long", "fips", "long"), ("num_licensed_beds", "long", "num_licensed_beds", "long"), ("num_staffed_beds", "long", "num_staffed_beds", "long"), ("num_icu_beds", "long", "num_icu_beds", "long"), ("adult_icu_beds", "long", "adult_icu_beds", "long"), ("pedi_icu_beds", "long", "pedi_icu_beds", "long"), ("bed_utilization", "double", "bed_utilization", "double"), ("potential_increase_in_bed_capac", "long", "potential_increase_in_bed_capac", "long"), ("avg_ventilator_usage", "long", "avg_ventilator_usage", "long")], transformation_ctx = "applymapping1")
## @type: SelectFields
## @args: [paths = ["x", "y", "objectid", "hospital_name", "hospital_type", "hq_address", "hq_address1", "hq_city", "hq_state", "hq_zip_code", "county_name", "state_name", "state_fips", "cnty_fips", "fips", "num_licensed_beds", "num_staffed_beds", "num_icu_beds", "adult_icu_beds", "pedi_icu_beds", "bed_utilization", "potential_increase_in_bed_capac", "avg_ventilator_usage"], transformation_ctx = "selectfields2"]
## @return: selectfields2
## @inputs: [frame = applymapping1]
selectfields2 = SelectFields.apply(frame = applymapping1, paths = ["x", "y", "objectid", "hospital_name", "hospital_type", "hq_address", "hq_address1", "hq_city", "hq_state", "hq_zip_code", "county_name", "state_name", "state_fips", "cnty_fips", "fips", "num_licensed_beds", "num_staffed_beds", "num_icu_beds", "adult_icu_beds", "pedi_icu_beds", "bed_utilization", "potential_increase_in_bed_capac", "avg_ventilator_usage"], transformation_ctx = "selectfields2")
## @type: ResolveChoice
## @args: [choice = "MATCH_CATALOG", database = "database-usa-hospital-beds", table_name = "hospitalusa_hospital_beds_csv", transformation_ctx = "resolvechoice3"]
## @return: resolvechoice3
## @inputs: [frame = selectfields2]
resolvechoice3 = ResolveChoice.apply(frame = selectfields2, choice = "MATCH_CATALOG", database = "database-usa-hospital-beds", table_name = "hospitalusa_hospital_beds_csv", transformation_ctx = "resolvechoice3")
## @type: DataSink
## @args: [database = "database-usa-hospital-beds", table_name = "hospitalusa_hospital_beds_csv", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = resolvechoice3]
datasink4 = glueContext.write_dynamic_frame.from_catalog(frame = resolvechoice3, database = "database-usa-hospital-beds", table_name = "hospitalusa_hospital_beds_csv", transformation_ctx = "datasink4")
job.commit()

