FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends p7zip-full libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["sleep", "infinity"]
