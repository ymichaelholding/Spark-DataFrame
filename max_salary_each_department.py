from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as f

emp_schmea=StructType([StructField("EMPLOYEE_ID",IntegerType(),False),
                           StructField("FIRST_NAME",StringType(),True),
                           StructField("LAST_NAME",StringType(),True),
                           StructField("EMAIL",StringType(),True),
                           StructField("PHONE_NUMBER",StringType(),True),
                           StructField("HIRE_DATE",DateType(),True),
                           StructField("JOB_ID",StringType(),True),
                           StructField("SALARY",IntegerType(),True),
                           StructField("COMMISSION_PCT",StringType(),True),
                           StructField("MANAGER_ID",IntegerType(),True),
                           StructField("DEPARTMENT_ID",IntegerType(),True),
                          ])
                          
emp_df = spark.read.option("header",True) \
            .schema(emp_schmea) \
            .option("dateFormat", "d-MMM-yy") \
            .csv('/FileStore/tables/employees-1.csv')

display(emp_df.groupBy("DEPARTMENT_ID").agg(max('SALARY')).alias("max_salary")\
.orderBy(asc(emp_df.DEPARTMENT_ID)));
