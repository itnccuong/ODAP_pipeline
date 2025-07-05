ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.9

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Switch to root to install Java (needed for Spark job submission)
USER root
RUN apt-get update && apt-get install -y \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark job submission
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to airflow user
USER airflow

# Install ONLY Airflow providers and utilities
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt && \
    rm ./requirements.txt

# Set Python path for custom plugins
ENV PYTHONPATH="$PYTHONPATH:$AIRFLOW_HOME/plugins"
