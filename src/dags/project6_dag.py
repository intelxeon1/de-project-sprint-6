
import pendulum
from utils import download_from_s3,load_stg
from const import AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,conn_info,S3_URL,DATA_DIRECTORY_PATH

from airflow.decorators import dag, task


@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['project_6', 'stg'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False, # Остановлен/запущен при появлении. Сразу запущен.
    dag_id = 'project_6'
)

def project6_dag():
    @task(task_id='fetch_s3_data')
    def task_fetch():    
        download_from_s3(S3_URL,AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,"group_log.csv",DATA_DIRECTORY_PATH)
    
    @task(task_id='upload_staging')
    def upload():
        load_stg(conn_info,"INTELXEONYANDEXRU__STAGING.group_log",f"{DATA_DIRECTORY_PATH}/group_log.csv")
    
    task_fetch() >> upload()
    
dag = project6_dag()

if __name__ == "__main__":
   dag.test()