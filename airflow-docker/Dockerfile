FROM apache/airflow:slim-2.9.0

USER airflow
COPY requirements.txt /opt/app/requirements.txt
WORKDIR /opt/app
RUN pip install -r requirements.txt
COPY . /opt/app

USER root
RUN chown -R airflow /opt/app/
RUN chmod 755 /opt/app
USER airflow
