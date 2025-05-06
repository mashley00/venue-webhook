# Use Python slim base image
FROM python:3.11-slim

# Prevents Python from writing .pyc files to disc & buffers logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Start Uvicorn server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
