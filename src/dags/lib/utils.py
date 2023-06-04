import boto3
import vertica_python


def download_from_s3(
    end_point: str, key_id: str, access_key: str, file_name: str, dir_path: str
):
    session = boto3.Session()

    s3_client = session.client(
        service_name="s3",
        endpoint_url=end_point,
        aws_access_key_id=key_id,
        aws_secret_access_key=access_key,
    )

    s3_client.download_file(
        Bucket="sprint6", Key=file_name, Filename=f"{dir_path}/{file_name}"
    )
    
def load_stg(conn_info,table_name:str,file_path:str):
    with vertica_python.connect(**conn_info) as conn:        
        cur = conn.cursor()
        cur.execute(f"truncate table {table_name}")
        cur.execute(f"""                
                    copy {table_name} from local '{file_path}' DELIMITER ',';
                    """)    