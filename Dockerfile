FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN pip install poetry

# Copy only dependency files
COPY pyproject.toml poetry.lock ./

# Configure poetry and install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-root

# Copy application code
COPY src/ ./src/
COPY src/schema.sql ./

CMD ["python", "src/main.py"]

# FROM python:3.12
# RUN pip install poetry requests
# COPY . /src
# WORKDIR /src
# RUN poetry install --no-root
# EXPOSE 8501
# ENTRYPOINT ["poetry", "run", "python", "src/main.py", "--server.port=8501", "--server.address=0.0.0.0"]