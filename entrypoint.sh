#!/bin/bash

# Start the Redis server with the RDB file saved to /data (host-mounted directory)
redis-server --appendonly yes --dir /data &

# Wait for Redis to start
sleep 5

# Run the Scrapy crawler
cd kanoon_scraper
scrapy crawl indiankanoon

# Shut down the Redis server
redis-cli shutdown save

