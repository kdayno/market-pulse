FROM quay.io/astronomer/astro-runtime:12.7.1

USER root
RUN apt-get update && apt-get install -y git
RUN git clone https://github.com/kdayno/market-pulse.git

WORKDIR /usr/local/airflow/market-pulse

RUN python3 -m venv .venv
SHELL ["/bin/bash", "-c"]
RUN source .venv/bin/activate

RUN mv ./market_pulse_dbt/profiles.yml.template ./market_pulse_dbt/profiles.yml

WORKDIR /usr/local/airflow

ENV AIRFLOW__SCHEDULER__SCHEDULER_ZOMBIE_TASK_THRESHOLD=900