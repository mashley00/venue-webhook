# Use slim Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Run the app on correct host and port
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "10000"]
