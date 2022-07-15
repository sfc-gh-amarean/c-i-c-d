#!/usr/bin/env python

#!/usr/bin/env python

import os
import snowflake.connector
import pandas as pd 
import boto3
from botocore.exceptions import ClientError
import json
import re
import requests

# Load Snowflake connection details from AWS Secrets Manager
def get_sf_connection_details(secret_name,region_name):  
  boto3_session = boto3.session.Session()
  client = boto3_session.client(service_name='secretsmanager',region_name=region_name)
  
  get_secret_value_response = None

  try:
    # Get secret values(s) based on the passed in secret name
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)['SecretString']
  except ClientError as e:
    if e.response['Error']['Code'] == 'DecryptionFailureException':
        # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
        raise e
    elif e.response['Error']['Code'] == 'InternalServiceErrorException':
        # An error occurred on the server side.
        raise e
    elif e.response['Error']['Code'] == 'InvalidParameterException':
        # You provided an invalid value for a parameter.
        raise e
    elif e.response['Error']['Code'] == 'InvalidRequestException':
        # You provided a parameter value that is not valid for the current state of the resource.
        raise e
    elif e.response['Error']['Code'] == 'ResourceNotFoundException':
        # We can't find the resource that you asked for.
        raise e

  return get_secret_value_response

def update():
  connection_params = json.loads(get_sf_connection_details('dash-sfdevrel-connection','us-west-2'))
  try:
    ctx = snowflake.connector.connect(
      user=connection_params['user'],
      password=connection_params['password'],
      account=connection_params['account'],
      role=connection_params['role'],
      warehouse=connection_params['warehouse'],
      database=connection_params['database'],
      schema=connection_params['schema']
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
          udf_db == connection_params['database'] and 
          udf_schema == connection_params['schema']):

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
