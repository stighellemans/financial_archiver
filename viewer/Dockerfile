FROM python:3.9-bullseye

# Install Math + Database libs
RUN apt-get update && apt-get install -y \
    build-essential \
    libopenblas-dev \
    libatlas-base-dev \
    libatomic1 \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip

# Install requirements. 
# We don't strictly need --no-binary if we use the pinned versions above, 
# but it doesn't hurt to be safe if you have time to wait for the build.
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]