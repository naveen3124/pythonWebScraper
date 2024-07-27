#!/bin/bash

# Start the Redis server with the RDB file saved to /data (host-mounted directory)
redis-server --appendonly yes --dir /data

# Run the Scrapy crawl
cd kanoon_scraper
scrapy crawl indiankanoon

# Stop Redis server
redis-cli shutdown save

# Exit the script
exit 0

