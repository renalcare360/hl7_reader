#imports
import psycopg2, sys, json, boto3
from botocore.exceptions import ClientError

def get_creds(dbname):
    if dbname == 'prod':
        secret_name = "prod-sa_prod_writer"
    else: 
        secret_name = "pg-sghantasala"


    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name="us-east-1"
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    creds =  json.loads((get_secret_value_response['SecretString']))
    return creds
    # return 

def connect(dbname = 'prod'):
    conn = None
    db_params = get_creds(dbname)
    db_params['dbname'] = dbname

    try:
        conn = psycopg2.connect(**db_params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    
    return conn
    