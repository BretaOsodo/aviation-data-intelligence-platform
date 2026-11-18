FROM apache/airflow:3.0.0

USER airflow

# Core pipeline dependencies + spaCy
# Airflow 3.0 image: pip is at /home/airflow/.local/bin/pip (user install path)
# Do NOT use --user flag — the build environment resolves pip correctly for the airflow user
RUN pip install \
    requests==2.31.0 \
    beautifulsoup4==4.12.3 \
    psycopg2-binary==2.9.9 \
    "dbt-core==1.8.7" \
    "dbt-postgres==1.8.2" \
    pandas==2.2.2 \
    spacy==3.7.4 \
    pytest==9.0.3 \
    python-dotenv==1.0.1 \
    openpyxl==3.1.4

# Download spaCy English model (free, no API key required)
RUN python -m spacy download en_core_web_sm