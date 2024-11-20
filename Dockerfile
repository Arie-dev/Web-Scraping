FROM apache/airflow:2.9.1

RUN pip install minio
RUN pip install textblob
RUN pip install pandas
RUN pip install numpy
RUN pip install streamlit