FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir "streamlit>=1.50" pandas requests

COPY weather_dashboard.py .

EXPOSE 8501

CMD ["streamlit", "run", "weather_dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
 