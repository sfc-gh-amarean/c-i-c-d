#!/usr/bin/env python

import os
import snowflake.connector
import re
import requests

def update():
  try:
    ACT = os.getenv('SNOWSQL_ACT')
    USR = os.getenv('SNOWSQL_USR')
    PWD = os.getenv('SNOWSQL_PWD')
    ROL = os.getenv('SNOWSQL_ROL')
    DBT = os.getenv('SNOWSQL_DBT')
    WRH = os.getenv('SNOWSQL_WRH')
    SCH = os.getenv('SNOWSQL_SCH')

    ctx = snowflake.connector.connect(
      user=USR,
      password=PWD,
      account=ACT,
      role=ROL,
      warehouse=WRH,
      database=DBT,
      schema=SCH
    )

    cur = ctx.cursor()
    url = "https://raw.githubusercontent.com/iamontheinet/user-defined-functions/main/do_something_cool.py"
    custom_udf_file = "do_something_cool.py"
    stage_name = "@dash_files"

    custom_udf_content = requests.get(url)
    open(custom_udf_file, 'wb').write(custom_udf_content.content)
    cur.execute(f"PUT file://{custom_udf_file} {stage_name} OVERWRITE=True;")
    print(f"Uploaded new custom UDF file {custom_udf_file}")

    print(f"List of affected UDFs...")
    udfs_sql = f"SHOW USER FUNCTIONS"
    for (udf_created_at, udf_name, udf_schema, col4, col5, col6, col7, col8, udf_signature, udf_description, udf_db, col12, col13, col14, col15, udf_language) in cur.execute(udfs_sql):
      if (udf_language == "PYTHON" and 
          udf_description == "user-defined function" and 
          udf_db == DBT and 
          udf_schema == SCH):

        #Extract UDF Name and Arguments 
        #-- Regex input  "PREDICT_SALES(NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER) RETURN VARIANT"
        #-- Regex output "PREDICT_SALES(NUMBER, NUMBER, NUMBER, NUMBER, NUMBER, NUMBER)"
        udf_signature = re.findall(r"(.*?)RETURN",udf_signature)[0] 
        desc_udf_sql = f"DESCRIBE FUNCTION {udf_signature}"
        for (prop, val) in cur.execute(desc_udf_sql):
          if (prop == "imports" and val.find(custom_udf_file) != -1):
            print(f">>> {udf_name}")
  finally:
    ctx.close()

if __name__ == "__main__":
  update()
