# Use a modern, slim Python runtime for better performance
FROM python:3.12-slim

# Set environment variables for Python optimization
# Prevents Python from writing pyc files to disk
ENV PYTHONDONTWRITEBYTECODE 1
# Prevents Python from buffering stdout and stderr (ensures logs appear immediately)
ENV PYTHONUNBUFFERED 1

# Set the working directory
WORKDIR /app

# Create a non-root user and switch to it (Security Best Practice)
RUN useradd -m appuser
USER appuser

# Copy and install requirements
# Use --chown to ensure the non-root user owns the files and optimize image layers
COPY --chown=appuser:appuser requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY --chown=appuser:appuser . .

# Cloud Run configuration
ENV PORT 8080

# Run the application using gunicorn with its full path to avoid PATH issues
CMD ["/home/appuser/.local/bin/gunicorn", "--bind", ":8080", "--workers", "1", "--threads", "8", "--timeout", "0", "ingestion_main:app"]
