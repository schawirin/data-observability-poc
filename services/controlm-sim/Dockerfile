FROM python:3.11-slim

WORKDIR /app

# Install system deps: docker CLI + FreeTDS (for pymssql/SQL Server)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates freetds-dev gcc && \
    curl -fsSL https://download.docker.com/linux/static/stable/aarch64/docker-27.5.1.tgz | \
    tar xz --strip-components=1 -C /usr/local/bin docker/docker && \
    apt-get remove -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "web.py"]
