from pyspark.sql.types import (DecimalType, IntegerType, StringType, StructField,
                               StructType, TimestampType)


#COMPANIES_SCHEMA = StructType([
#    StructField("country_code", StringType(), True),
#    StructField("registry_id", StringType(), True),
#    StructField("legal_name", StringType(), False),
#    StructField("legal_form_code", StringType(), True),
#    StructField("legal_form_label", StringType(), True),
#    StructField("status", StringType(), True),
#    StructField("industry_code", StringType(), False),
#    StructField("founding_date", TimestampType(), True),
#    StructField("postal_code", StringType(), True),
#    StructField("city", StringType(), True),
#])


COMPANIES_SCHEMA = StructType([
    StructField("CountryOfOrigin", StringType(), True),
    StructField("CompanyNumber", StringType(), True),
    StructField("CompanyName", StringType(), False),
    StructField("CompanyCategory", StringType(), True),
    StructField("legal_form_label", StringType(), True),
    StructField("CompanyStatus", StringType(), True),
    StructField("SICCode.SicText_1", StringType(), False),
    StructField("IncorporationDate", TimestampType(), True),
    StructField("RegAddress.PostCode", StringType(), True),
    StructField("city", StringType(), True),
])


