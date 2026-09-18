FROM apache/airflow:3.0.6

RUN pip install --no-cache-dir \
    "numpy==1.26.4" \
    "pandas==2.1.4" \
    "scipy==1.13.1" \
    "ydata-profiling==4.18.4" \
    "psycopg2-binary"