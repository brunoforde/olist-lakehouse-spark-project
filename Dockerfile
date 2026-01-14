# Use official lightweight Python image
FROM python:3.9-slim

# Install OpenJDK 11 (Required for Spark) and system utilities
RUN apt-get update && \
    apt-get install -y default-jdk procps && \
    apt-get clean

# Set working directory inside the container
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code
# COPY . .

# Default command to run the smoke test
CMD ["python", "src/jobs/test_spark.py"]