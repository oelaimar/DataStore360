from statistics import correlation

from airflow.sdk import dag, task

ROOT_DIR = "/opt/airflow"

@dag(description="extract -> cleaning -> transform -> loading",schedule=None)

def etl_pipline():
    @task
    def extract():

        INPUT_PATH =  f"{ROOT_DIR}/data/raw/store-data.csv"
        OUTPUT_PATH = f"{ROOT_DIR}/data/processed/data_cleaned.csv"
        import pandas as pd
        df = pd.read_csv(INPUT_PATH)
        df.to_csv(OUTPUT_PATH, index=False)

        return OUTPUT_PATH

    @task
    def profiling(file_path):
        import pandas as pd
        from ydata_profiling import ProfileReport
        df = pd.read_csv(file_path)
        profile = ProfileReport(df, title="Data Profiling Report")
        profile.to_file(f"{ROOT_DIR}/reports/rapport_profiling.html")

    profiling(extract())

etl_pipline()