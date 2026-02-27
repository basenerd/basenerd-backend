# Dockerfile — use python 3.11 slim so wheels availability is stable
FROM python:3.11-slim

# system deps that help manylinux wheels (keeps image small)
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    build-essential ca-certificates libpq-dev curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Upgrade pip and install only binary wheels (no compiling)
RUN python -m pip install --upgrade pip setuptools wheel \
 && pip install --only-binary=:all: -r requirements.txt

# Copy the rest of the repo
COPY . .

# Expose port that Render expects (10000 or 8080 depending on your service)
ENV PORT=10000

# Default command to run your web app (adjust if you use a different module)
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:10000"]
