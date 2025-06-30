FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY app.py ./

EXPOSE $PORT

CMD ["streamlit", "run", "app.py", "--server.port", "$PORT", "--server.enableCORS", "false", "--server.enableXsrfProtection", "false"]
