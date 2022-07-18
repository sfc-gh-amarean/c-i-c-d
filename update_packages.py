#!/usr/bin/env python

import os
import snowflake.connector
import re
import requests
import json

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

    # Load list of packages to be uploaded
    with open("packages_list.json", "r") as packages_list:
      packages = json.load(packages_list)

    # Loop through each package and upload it to the stage and also examine which UDFs are affected by and use the package
    for pkg in packages:
      package_file_url = pkg['url']
      package_file_name = pkg['name']
      stage_name = pkg['stage']

      package_content = requests.get(package_file_url)
      open(package_file_name, 'wb').write(package_content.content)
      cur.execute(f"PUT file://{package_file_name} @{stage_name} OVERWRITE=True;")
      print(f"***** Uploaded new package {package_file_name} to stage @{stage_name}")
      os.remove(package_file_name)

      print(f"***** List of UDFs that use package {package_file_name}")
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
            if (prop == "imports" and val.find(package_file_name) != -1):
              print(f">>>>>>>>>> {udf_name}")
  finally:
    ctx.close()

if __name__ == "__main__":
  update()
