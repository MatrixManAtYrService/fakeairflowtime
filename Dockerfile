FROM quay.io/astronomer/ap-airflow-dev:2.2.0-buster-43536

USER root

# deps for faking time and talking to postgres databases
RUN apt-get update && apt-get install -y faketime libpq-dev build-essential

# place shim where airflow was, `airflow` is now `actual_airflow`
RUN sh -c 'set -x ; mv $(which airflow) $(dirname $(which airflow))/actual_airflow'
COPY ./airflow_wrapper.sh /home/root/
COPY ./containertime.py /home/root/
RUN chmod +x /home/root/airflow_wrapper.sh /home/root/containertime.py
RUN sh -c 'set -x; cp /home/root/airflow_wrapper.sh $(dirname $(which actual_airflow))/airflow'
RUN sh -c 'set -x; cp /home/root/containertime.py $(dirname $(which actual_airflow))/containertime.py'
COPY requirements.txt /home/astro
USER astro
RUN pip install -r /home/astro/requirements.txt

ENV FIRST_FAKED_TIME=2021-10-01
