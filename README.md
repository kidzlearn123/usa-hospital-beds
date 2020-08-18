# usa-hospital-beds
subscribed to "USA Hospital Beds - COVID-19 | Definitive Healthcare" delivered by “Rearc”  using Amazon AWS Data Exchange. New datasets are added every day for this product, so I created a CloudFormation distribution from an existing template to update the data in S3 from AWS Data Exchange.  The CloudFormation created a stack by creating resources like S3 bucket to store the hospital bed data, Lambda functions to manage daily update of data in the S3 bucket, IAM Permission Roles and events.
The datasets that are in .CSV and .GeoJSON format is extracted from S3 and tables are created using Extract, Transform, Load function and crawlers in AWS Glue. I created a trigger "ETL_Trigger_COVID_Beds" and scheduled a job  "JOB_ETL_COVID_BED" to run every once a week to update the table contents. The job uses a python script to map the fields from S3 to Athena. The database "database-usa-hospital-beds" with table name "hospitalusa_hospital_beds_csv" is created in Athena.
For data visualization and analysis, I used AWS QuickSight. The input data for generating visualizations in QuickSight is from Athena.

