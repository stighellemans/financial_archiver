FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
# Create downloads directory inside container
RUN mkdir downloads
CMD ["python", "main.py"]