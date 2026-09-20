FROM apache/airflow:3.0.6

RUN pip install --no-cache-dir \
    "numpy==1.26.4" \
    "pandas==2.1.4" \
    "ydata-profiling==4.18.4" \
    "apache-airflow-providers-postgres"