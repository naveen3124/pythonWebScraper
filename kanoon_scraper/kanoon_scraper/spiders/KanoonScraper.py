import scrapy
import redis
from urllib.parse import urlparse, urljoin
import re
import snappy
import zlib

id_to_doc_map = "id_to_doc_map"
doc_to_cited_doc_idmap = "doc_to_cited_doc_idmap"
last_doc_crawled = 'last_doc_crawled'
kanoon_judgements_maps = "kanoon_judgements_maps"

# don't change order
kanoon_judgement_schema = ['docsource_main', 'doc_title', 'doc_bench', 'doc_author', 'judgement']
class IndianKanoonSpider(scrapy.Spider):
    name = "indiankanoon"
    allowed_domains = ['indiankanoon.org']
    custom_settings = {
        'DOWNLOAD_DELAY': 6,
        'CONCURRENT_REQUESTS': 1,
        'DEPTH_LIMIT': 100,
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    }

    def url_generator(self, start, stop, step=1):
        current = start
        while current <= stop:
            yield f'https://indiankanoon.org/doc/{current}/'
            self.redis_connection.incr(last_doc_crawled)
            current += step

    def start_requests(self):
        # Generate dynamic URLs using the generator function
        if self.redis_connection.exists(last_doc_crawled):
            # If it exists, set the initial value to the existing value
            current_doc_id = int(self.redis_connection.get(last_doc_crawled)) + 1
        else:
            current_doc_id = 1
        dynamic_urls_generator = self.url_generator(current_doc_id, 1)  # Range from 1 to 10,000,000

        # Create Request objects for each URL in the generator
        for url in dynamic_urls_generator:
            yield scrapy.Request(url=url, callback=self.parse)

    # Class attribute for Redis connection
    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(IndianKanoonSpider, cls).from_crawler(crawler, *args, **kwargs)

        # Attempt to set up the Redis connection
        try:
            spider.redis_connection = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)


        except redis.ConnectionError:
            spider.logger.error("Failed to connect to Redis. Stopping the spider.")
            spider.close(reason="Failed to connect to Redis")

        return spider

    def is_valid_url(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def has_same_domain(self, url):
        start_domain = urlparse(self.start_urls[0]).netloc
        url_domain = urlparse(url).netloc
        return start_domain == url_domain

    def __init__(self, *args, **kwargs):
        super(IndianKanoonSpider, self).__init__(*args, **kwargs)
        # Initialize the set key in Redis
        self.parsed_set_key = 'parsed_document_numbers_set'

    def get_redis_docset(self, set_name):
        # Check if the set exists
        if self.redis_conn.exists(set_name):
            # Fetch the existing set
            existing_set = self.redis_conn.smembers(set_name)
            print(f"Existing Set: {existing_set}")
            return existing_set
        else:
            # Create a new set
            return None

    def parse_view_all(self, response, cited_docs):
        # Process the response from the "View All" page as needed
        for result in response.css('.result_title'):
            result_title = result.css('.result_title a::text').get().strip()
            a_tag = result.css('a')

            # Extract the text within the href attribute
            href_text = a_tag.css('::attr(href)').get()
            match = re.search(r'/doc(?:fragment)?/(\d+)/', href_text)
            doc_number = 0
            if match:
                doc_number = match.group(1)
            self.redis_connection.hset(id_to_doc_map, doc_number, result_title)
            cited_docs.append(doc_number)

        next_url = response.css('div.bottom a:contains("Next")::attr(href)').extract_first()

        if next_url:
            next_url = response.urljoin(next_url)
            # print(f'Next URL: {next_url}')
            yield scrapy.Request(url=next_url, callback=self.parse_view_all, cb_kwargs={'cited_docs': cited_docs})

        if next_url is None:
            self.redis_connection.hset(doc_to_cited_doc_idmap, 'your_field', ','.join(cited_docs))
            print(f'number of citations  {len(cited_docs)}')

    def parse(self, response):

        try:
            parsed_url = urlparse(response.url)
            doc_number = parsed_url.path.split("/")[-2]
            print(doc_number)
            for judgment_div in response.css('div.judgments'):

                text_content = ''.join(judgment_div.css('p::text').getall())
                text_content = re.sub(r'\n\s*\n*', '\n', text_content).encode('utf-8')
                compressed_bytes = zlib.compress(text_content)

                item = {
                    kanoon_judgement_schema[0]: judgment_div.css('div.docsource_main::text').get(default='EMPTY'),
                    kanoon_judgement_schema[1]: judgment_div.css('div.doc_title::text').get(default='EMPTY'),
                    kanoon_judgement_schema[2]: judgment_div.css('div.doc_bench::text').get(default='EMPTY'),
                    kanoon_judgement_schema[3]: judgment_div.css('div.doc_author::text').get(default='EMPTY'),
                    kanoon_judgement_schema[4]: compressed_bytes
                }
                # Decompress the compressed bytes
                # decompressed_bytes = zlib.decompress(compressed_bytes)
                # Convert the decompressed bytes back to text
                #decompressed_text = decompressed_bytes.decode('utf-8')

                # Handle empty text or non-existent fields
                item = {k: v if v is not None else 'EMPTY' for k, v in item.items()}

                # Use hset to update individual fields within the same hash
                field_key = f'{doc_number}'
                for key, value in item.items():
                    self.redis_connection.hset(kanoon_judgements_maps, field_key + '_' + key, value)
            # Extract information from the current page
            view_all_link = response.css('div.doc_cite a:contains("View All")::attr(href)').extract_first()
            cited_docs = []
            if view_all_link:
                full_url = response.urljoin(view_all_link)
                yield scrapy.Request(url=full_url, callback=self.parse_view_all, cb_kwargs={'cited_docs': cited_docs})
        except redis.exceptions.RedisError as e:
            self.log(f"Redis error: {e}")
