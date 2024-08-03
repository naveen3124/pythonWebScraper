FROM python:3.9-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    redis-server \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages from requirements.txt

# Expose the Redis port
EXPOSE 6379

COPY kanoon_scraper /app/kanoon_scraper
COPY entrypoint.sh /app/entrypoint.sh
COPY requirements.txt /app/requirements.txt
COPY scrapy.cfg /app/scrapy.cfg

RUN pip install --no-cache-dir -r /app/requirements.txt
# Make the startup script executable
RUN chmod +x /app/entrypoint.sh

# Set the working directory
WORKDIR /app

# Set the entrypoint to the startup script
ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
