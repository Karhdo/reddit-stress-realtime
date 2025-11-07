FROM --platform=linux/arm64 python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# 1. Cài JRE 17 headless (đủ cho Spark/Airflow), + CA certs, curl
#    Using option to avoid old cache / mismatch and retry on mirror failure
RUN set -eux; \
    echo 'Acquire::Retries "10";' > /etc/apt/apt.conf.d/80retries; \
    echo 'Acquire::http::No-Cache "true"; Acquire::https::No-Cache "true";' > /etc/apt/apt.conf.d/80no-cache; \
    echo 'Acquire::http::Pipeline-Depth "0";' > /etc/apt/apt.conf.d/80pipeline; \
    rm -rf /var/lib/apt/lists/*; \
    apt-get update; \
    apt-get install -y --no-install-recommends --fix-missing \
        openjdk-17-jre-headless \
        ca-certificates \
        curl; \
    rm -rf /var/lib/apt/lists/*

# 2. Setup JAVA_HOME dynamically based on 'java' binary (works for ARM64/AMD64)
#    Create alias default-java to keep PATH stable
RUN set -eux; \
    JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"; \
    ln -s "$JAVA_HOME" /usr/lib/jvm/default-java; \
    echo "Detected JAVA_HOME=$JAVA_HOME"; \
    echo "JAVA_HOME=/usr/lib/jvm/default-java" >> /etc/environment

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"
WORKDIR /app

# 3. Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. App source
COPY . .

# 5. Streamlit defaults
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
EXPOSE 8501

# 6. Entrypoint
CMD ["streamlit", "run", "apps/streamlit/app_streamlit.py", "--server.port=8501", "--server.address=0.0.0.0"]