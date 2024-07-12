# Use the official Airflow image
FROM apache/airflow:2.2.3

# Install additional packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Copy your scripts into the container
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY service-account.json opt/airflow/service-account.json

# Set environment variables for Google Cloud authentication
ENV GOOGLE_APPLICATION_CREDENTIALS="/opt/airflow/service-account.json"

# Set the working directory
WORKDIR /opt/airflow/scripts/
