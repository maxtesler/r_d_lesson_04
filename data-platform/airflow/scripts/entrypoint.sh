#!/bin/bash

# Function to replace environment variables in config file
replace_vars() {
  local config_file=$1
  local temp_file="${config_file}.tmp"
  
  # Create a copy of the original file
  cp $config_file $temp_file
  
  # Replace environment variables
  sed -i "s|\$(AIRFLOW_FERNET_KEY)|${AIRFLOW_FERNET_KEY}|g" $temp_file
  sed -i "s|\$(POSTGRES_AIRFLOW_USER)|${POSTGRES_AIRFLOW_USER}|g" $temp_file
  sed -i "s|\$(POSTGRES_AIRFLOW_PASSWORD)|${POSTGRES_AIRFLOW_PASSWORD}|g" $temp_file
  sed -i "s|\$(POSTGRES_AIRFLOW_HOST)|${POSTGRES_AIRFLOW_HOST}|g" $temp_file
  sed -i "s|\$(POSTGRES_AIRFLOW_DB)|${POSTGRES_AIRFLOW_DB}|g" $temp_file
  sed -i "s|\$(ANALYTICS_DB)|${ANALYTICS_DB}|g" $temp_file
  
  # Move the temp file back to the original
  mv $temp_file $config_file
}

# Replace variables in the config file
replace_vars /opt/airflow/airflow.cfg

# Wait for Airflow PostgreSQL
echo "Waiting for Airflow PostgreSQL..."
while ! nc -z $POSTGRES_AIRFLOW_HOST 5432; do
  sleep 0.5
done
echo "Airflow PostgreSQL started"

# Wait for Analytics PostgreSQL
# echo "Waiting for Analytics PostgreSQL..."
# while ! nc -z $POSTGRES_ANALYTICS_HOST 5432; do
#   sleep 0.5
# done
# echo "Analytics PostgreSQL started"

# Install dbt packages if they don't exist
if [ -f /opt/airflow/dags/dbt/homework/packages.yml ] && [ ! -d /opt/airflow/dbt/dbt_packages ]; then
  echo "Installing dbt packages..."
  cd /opt/airflow/dags/dbt && dbt deps --profiles-dir /opt/airflow/dags/dbt --project-dir /opt/airflow/dags/dbt/homework
  echo "dbt packages installed successfully"
fi

# Initialize the database
airflow db init

# Create default user if needed
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Add connections for ETL tasks
airflow connections add 'postgres_analytics' \
    --conn-type 'postgres' \
    --conn-host "$POSTGRES_ANALYTICS_HOST" \
    --conn-login "$ETL_USER" \
    --conn-password "$ETL_PASSWORD" \
    --conn-schema "$ANALYTICS_DB" \
    --conn-port "5432"

# Add connection for readonly access
airflow connections add 'postgres_analytics_readonly' \
    --conn-type 'postgres' \
    --conn-host "$POSTGRES_ANALYTICS_HOST" \
    --conn-login "$ANALYTICS_READONLY_USER" \
    --conn-password "$ANALYTICS_READONLY_PASSWORD" \
    --conn-schema "$ANALYTICS_DB" \
    --conn-port "5432"

# Start Airflow based on the command
exec airflow "$@"