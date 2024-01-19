import base64

import scrapy
import redis
from kanoon_scraper.items import CaseItem, CasesRefererItem, CasesReferredItem

from urllib.parse import urlparse, urljoin
import re
import zlib

id_to_doc_map = "id_to_doc_map"
id_to_referer_map = "id_to_referer_map"
id_to_referred_map = "id_to_referred_map"


def read_counter(filename='counter.txt'):
    try:
        with open(filename, 'r') as file:
            return int(file.read())
    except FileNotFoundError:
        return 0


def write_counter(counter, filename='counter.txt'):
    with open(filename, 'w') as file:
        file.write(str(counter))


class IndianKanoonSpider(scrapy.Spider):
    name = "indiankanoon"
    allowed_domains = ['indiankanoon.org']

    def url_generator(self, start, stop, step=1):
        current = start
        while current <= stop:
            yield f'https://indiankanoon.org/doc/{current}/'
            current += step

    def start_requests(self):
        # Generate dynamic URLs using the generator function
        current_doc_id = read_counter()
        dynamic_urls_generator = self.url_generator(current_doc_id, 2)  # Range from 1 to 10,000,000
        for url in dynamic_urls_generator:
            yield scrapy.Request(url=url, callback=self.parse)
            current_doc_id += 1
            write_counter(current_doc_id)

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

    def parse_view_all_cites(self, response, doc_number, cited_docs):
        # Process the response from the "View All" page as needed
        for result in response.css('.result_title'):
            a_tag = result.css('a')
            # Extract the text within the href attribute
            href_text = a_tag.css('::attr(href)').get()
            match = re.search(r'/doc(?:fragment)?/(\d+)/', href_text)
            if match:
                cites_doc_no = match.group(1)
                cited_docs.append(cites_doc_no)

        next_url = response.css('div.bottom a:contains("Next")::attr(href)').extract_first()

        if next_url:
            next_url = response.urljoin(next_url)
            # print(f'Next URL: {next_url}')
            yield scrapy.Request(url=next_url, callback=self.parse_view_all_cites,
                                 cb_kwargs={'doc_number': doc_number, 'cited_docs': cited_docs})

        if next_url is None:
            # self.redis_connection.hset(doc_to_cited_doc_idmap, 'your_field', ','.join(cited_docs))
            print(f'number of citations  {len(cited_docs)}')
            item = CasesRefererItem()
            item["stored_hset_name"] = id_to_referer_map
            item['case_id'] = doc_number
            item['case_cites'] = cited_docs
            yield item

    def parse_view_all_cited(self, response, doc_number, cited_docs):
        # Process the response from the "View All" page as needed
        for result in response.css('.result_title'):
            a_tag = result.css('a')
            # Extract the text within the href attribute
            href_text = a_tag.css('::attr(href)').get()
            match = re.search(r'/doc(?:fragment)?/(\d+)/', href_text)
            if match:
                cited_doc_no = match.group(1)
                cited_docs.append(cited_doc_no)

        next_url = response.css('div.bottom a:contains("Next")::attr(href)').extract_first()

        if next_url:
            next_url = response.urljoin(next_url)
            # print(f'Next URL: {next_url}')
            yield scrapy.Request(url=next_url, callback=self.parse_view_all_cites,
                                 cb_kwargs={'doc_number': doc_number, 'cited_docs': cited_docs})

        if next_url is None:
            # self.redis_connection.hset(doc_to_cited_doc_idmap, 'your_field', ','.join(cited_docs))
            print(f'number of citations  {len(cited_docs)}')
            item = CasesReferredItem()
            item["stored_hset_name"] = id_to_referred_map
            item['case_id'] = doc_number
            item['case_cited_by'] = cited_docs
            yield item

    def parse(self, response):
        try:
            parsed_url = urlparse(response.url)
            doc_number = parsed_url.path.split("/")[-2]
            item = CaseItem()
            item["case_id"] = doc_number
            item["stored_hset_name"] = id_to_doc_map

            for judgment_div in response.css('div.judgments'):
                text_content = ''.join(judgment_div.css('p::text').getall())
                # TODO move all the cleaning tasks to the pipelines.py
                text_content = re.sub(r'\n\s*\n*', '\n', text_content).encode('utf-8')
                compressed_bytes = zlib.compress(text_content)
                item["case_source"] = judgment_div.css('h2.docsource_main::text').get(default='EMPTY')
                item["case_title"] = judgment_div.css('h2.doc_title::text').get(default='EMPTY')
                item["case_bench"] = judgment_div.css('div.doc_bench::text').get(default='EMPTY')
                item["case_author"] = judgment_div.css('div.doc_author::text').get(default='EMPTY')
                item["case_judgement"] = base64.b64encode(compressed_bytes).decode('utf-8')

                # Decompress the compressed bytes
                # decoded_data = base64.b64decode(compressed_data_base64)
                # decompressed_bytes = zlib.decompress(compressed_bytes)
                # Convert the decompressed bytes back to text
                # decompressed_text = decompressed_bytes.decode('utf-8')

            # Extract information from the current page
            yield item

            covers_div = response.css('div.covers')
            cites_href = covers_div.css('span.citetop a[href*="cites"]::attr(href)').extract_first()
            cited_by_href = covers_div.css('span.citetop a[href*="citedby"]::attr(href)').extract_first()

            # Now you can use 'cites_href' and 'cited_by_href'
            print("Cites Href:", cites_href)
            print("Cited By Href:", cited_by_href)
            cited_docs = []
            if cited_by_href:
                full_url = response.urljoin(cited_by_href)
                yield scrapy.Request(url=full_url, callback=self.parse_view_all_cited,
                                     cb_kwargs={'doc_number': doc_number, 'cited_docs': cited_docs})
            cited_by_docs = []
            if cites_href:
                full_url = response.urljoin(cites_href)
                yield scrapy.Request(url=full_url, callback=self.parse_view_all_cites,
                                     cb_kwargs={'doc_number': doc_number, 'cited_docs': cited_by_docs})

        except redis.exceptions.RedisError as e:
            self.log(f"Redis error: {e}")
