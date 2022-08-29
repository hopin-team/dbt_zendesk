FROM python:3.8.5
WORKDIR /dbt

COPY ./* /dbt/

# Update and install system packages
RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q git libpq-dev python-dev && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
  pip install --upgrade pip setuptools && \
  pip install dbt-databricks
  # git clone https://github.com/hopin-team/dbt_zendesk.git
  # cp -a ./* /dbt/dbt_zendesk

RUN pwd && \
  mkdir -p ~/.dbt && \
  cp dbt_zendesk/dbt_profiles.yml ~/.dbt/profiles.yml

# Set environment variables
ENV DBT_DIR /dbt/dbt_zendesk

# Set working directory
WORKDIR $DBT_DIR

# Run dbt
RUN dbt deps