FROM apache/airflow:3.2.2

# Extra libraries used by the DAGs, pinned in requirements.txt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
