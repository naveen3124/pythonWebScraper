import base64
import re
import zlib
from datetime import datetime
from urllib.parse import urlparse

import redis
import scrapy

from kanoon_scraper.items import CaseItem, CasesRefererItem, CasesReferredItem

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
        dynamic_urls_generator = self.url_generator(current_doc_id, 20)  # Range from 1 to 10,000,000
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
            yield scrapy.Request(url=next_url, callback=self.parse_view_all_cites,
                                 cb_kwargs={'doc_number': doc_number, 'cited_docs': cited_docs})

        if next_url is None:
            # print(f'number of citations  {len(cited_docs)}')
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
            yield scrapy.Request(url=next_url, callback=self.parse_view_all_cites,
                                 cb_kwargs={'doc_number': doc_number, 'cited_docs': cited_docs})

        if next_url is None:
            # print(f'number of citations  {len(cited_docs)}')
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
            soup = scrapy.Selector(response).xpath('//div[@class="judgments"]')
            if soup is None:
                return
            item["case_author"] = soup.xpath('//h3[@class="doc_author"]/a/text()').get()
            item["case_bench"] = soup.xpath('.//h3[@class="doc_bench"]/a/text()').get()
            item["case_source"] = soup.xpath('.//h2[@class="docsource_main"]/text()').get()
            item["case_title"] = soup.xpath('.//h2[@class="doc_title"]/text()').get()
            case_details = soup.xpath('//pre[@id="pre_1"]/text()').get()
            if case_details is not None:
                item["case_details"] = re.sub(r'\s+', ' ', case_details).strip()
            else:
                item["case_details"] = "EMPTY"

            paragraphs = soup.xpath('.//p[@id]')

            # Extract information and store in a list
            paragraphs_info = []
            for paragraph in paragraphs:
                paragraph_id = paragraph.xpath('@id').get()
                data_structure = paragraph.xpath('@data-structure').get()
                content = paragraph.xpath('string(.)').get()
                paragraphs_info.append(f"Par_ID: {paragraph_id}, "
                                       f"Data_Structure: {data_structure}, "
                                       f"Content: {content.strip()}\n")

            text_content = ''.join(paragraphs_info).encode('utf-8')
            if len(text_content) < 100:
                return
            compressed_bytes = zlib.compress(text_content)
            item["case_judgement"] = base64.b64encode(compressed_bytes).decode('utf-8')

            # Decompress the compressed bytes
            # decoded_data = base64.b64decode(item["case_judgement"])
            # decompressed_bytes = zlib.decompress(decoded_data)
            # Convert the decompressed bytes back to text
            # decompressed_text = decompressed_bytes.decode('utf-8')
            # print(decompressed_text)

            yield item

            covers_div = response.css('div.covers')
            cites_href = covers_div.css('span.citetop a[href*="cites"]::attr(href)').extract_first()
            cited_by_href = covers_div.css('span.citetop a[href*="citedby"]::attr(href)').extract_first()

            # Now you can use 'cites_href' and 'cited_by_href'
            # print("Cites Href:", cites_href)
            # print("Cited By Href:", cited_by_href)
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
            print(f" scraped doc_number {doc_number}")

        except redis.exceptions.RedisError as e:
            self.log(f"Redis error: {e}")

    def close(self, reason):
        # Print the current time at the end of crawling
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log(f"End of Crawling - Current Time: {current_time}")
