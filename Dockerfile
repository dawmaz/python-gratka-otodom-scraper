FROM python:3.8-slim

WORKDIR /usr/src/app
COPY . .

RUN apt-get update \
    && apt-get install -y wget \
    && apt-get autoremove -yqq --purge wget && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

CMD ["python3", "main.py"]