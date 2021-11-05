import os
import pickle
from datetime import datetime

import pymongo
import scrapy

from ultrabook_review.items import UltrabookReviewItem
from ultrabook_review.settings import (MONGODB_SERVER,
                                       MONGODB_COLLECTION,
                                       MONGODB_PORT, MONGODB_DB,
                                       OUTPUT_DATA_TO_FILE,
                                       WEBSITE_NAME)


class LaptopSpider(scrapy.Spider):
    PRODUCT_SOURCE_KEY = "Source"
    PRODUCT_ID = "id"
    MEDIA_TYPE_IMAGE = 'ImageObject'
    base_url = "https://www.ultrabookreview.com/"
    name = "ultrabook_laptops"
    output_dir = 'output'
    product_fetched_file_name = '.product_fetched.pkl'
    product_fetched_file_path = os.path.join(output_dir, product_fetched_file_name)
    output_file_name = 'output.xlsx'
    laptop_page_url = "https://www.ultrabookreview.com/"
    header_key = 'header_text_key'
    product_name_key = 'Product Name'
    product_category_key = 'Product Category'
    data_points_key = 'Data Points'
    image_prefix = 'Image count for'
    header_mappings = dict()
    products_fetched = set()
    headers = dict()

    def __init__(self, **kwargs):
        self.started_on = datetime.now()
        super().__init__(**kwargs)
        if OUTPUT_DATA_TO_FILE:
            self.get_fetched_products_from_file()
        else:
            self.get_fetched_products_from_mongo()

    def get_fetched_products_from_file(self):
        try:
            data = pickle.load(open(self.product_fetched_file_path, 'rb')) or dict()
            self.products_fetched = data['products_fetched']
        except (FileNotFoundError, KeyError):
            pass
        except Exception as err:
            print("Unable to load the fetched products list due to error: {}".format(err))

    def get_fetched_products_from_mongo(self):
        try:
            connection = pymongo.MongoClient(
                MONGODB_SERVER,
                MONGODB_PORT
            )
            db = connection[MONGODB_DB]
            products = db[MONGODB_COLLECTION]
            for product in products.find({}, {self.PRODUCT_SOURCE_KEY: 1, "_id": 0}):
                self.products_fetched.add(product.get(self.PRODUCT_SOURCE_KEY))
        except Exception as err:
            print("Unable to load the fetched products list due to error: {}".format(err))

    def start_requests(self):
        yield scrapy.Request(self.laptop_page_url, meta={'cur_page': 1})

    def parse(self, response, **kwargs):
        cur_page = int(response.meta.get('cur_page') or 1)
        laptops = response.css(".archive-text2 a")
        if not laptops:  # Final page will have no laptops
            return None
        for laptop in laptops:
            link = laptop.attrib['href']
            if link in self.products_fetched:
                continue
            self.products_fetched.add(link)
            yield scrapy.Request(link, callback=self.parse_laptop_details,
                                 meta={'laptop_fetch_cnt': len(self.products_fetched)})

        next_page = cur_page + 1
        next_page_url = self.laptop_page_url + '/page/{}'.format(next_page)
        yield scrapy.Request(next_page_url, meta={'cur_page': next_page})

    @classmethod
    def parse_specifications(cls, box, row, *_):
        rows = box.css("tr")
        for spec_row in rows[1:]:
            key_tag, value_tag, *_ = spec_row.css("td")
            key_list = key_tag.css("td ::text").extract()
            value_list = value_tag.css("td ::text").extract()
            key = "".join(key_list)
            value = "".join(value_list)
            row[key] = value

    @classmethod
    def parse_game_performance(cls, box, row, *_):
        rows = box.css("tr")
        key_tag, value_tag, *_ = rows[0].css("td")
        value_list = value_tag.css("td ::text").extract()
        value = "".join(value_list)
        if "FHD" not in value:
            return
        for spec_row in rows[1:]:
            key_tag, value_tag, *_ = spec_row.css("td")
            key_list = key_tag.css("td ::text").extract()
            value_list = value_tag.css("td ::text").extract()
            key = "".join(key_list)
            value = "".join(value_list)
            key = f"Game_{key}"
            row[key] = value

    def parse_table(self, box, row, *_):
        key_lower = str.lower(row[self.header_key])
        if "spec" in key_lower:
            self.parse_specifications(box, row, *_)
        elif "performance" in key_lower:
            self.parse_game_performance(box, row, *_)

    def get_image_item(self, tag):
        link_elem = tag.css('a')
        if link_elem.attrib.get('href') is None:
            return
        image_url = str(link_elem.attrib['href'])
        if not image_url.startswith(self.base_url):
            image_url = self.base_url + image_url
        return image_url, None

    def parse_main_image(self, box, row, images, *_):
        image_item = self.get_image_item(box)
        current_images = images.get(row[self.header_key]) or list()
        images[row[self.header_key]] = current_images + [image_item, ]

    def parse_product_images(self, box, row, images, *_):
        image_icons = box.css('.gallery-item>.gallery-icon')
        image_urls = list()
        for fig in image_icons:
            image_item = self.get_image_item(fig)
            image_item and image_urls.append(image_item)
        if image_urls:
            current_images = images.get(row[self.header_key]) or list()
            images[row[self.header_key]] = current_images + image_urls

    def parse_header_text(self, box, row, *_):
        header_text = box.css("h2 ::text").extract()
        row[self.header_key] = header_text and "".join(header_text)

    def skip_function(self, *args):
        pass

    @classmethod
    def parse_pros_and_cons(cls, box, row, *_):
        max_pros_cons = 10
        detail_boxes = [box.css('div.revgood'), box.css('.revbad')]
        detail_texts = ['Pro', 'Con']
        for detail_box, detail_text in zip(detail_boxes, detail_texts):
            detail_rows = detail_box.css('li')
            detail_len = len(detail_rows)
            for i in range(max_pros_cons):
                key = '{}_{}_text'.format(detail_text, i + 1)
                val = None
                if i < detail_len:
                    val = detail_rows[i].css('li ::text').get()
                row[key] = val and val.strip()

    def parse_laptop_details(self, response, **_):
        laptop_fetch_cnt = response.meta.get('laptop_fetch_cnt')
        product_row = {self.header_key: 'Specification', "fetched_from": WEBSITE_NAME}
        # Parse name
        product_name_elem = response.css('.headline::text').get()
        if not product_name_elem:
            print("Unable to find name!!")
            print("Source: {}".format(response.url))
            return
        product_name = ' '.join(product_name_elem.split()).strip('( ')
        if " review " not in product_name.lower():
            print("Not a laptop review")
            return
        product_row[self.product_name_key] = product_name
        product_row[self.product_category_key] = " "
        product_row[self.PRODUCT_SOURCE_KEY] = response.url
        product_row[self.PRODUCT_ID] = laptop_fetch_cnt

        product_row[self.data_points_key] = dict()

        images = dict()
        # Go through the boxes of content-area and parse them as per their type
        boxes = response.css('#content-area>*')
        identifiers_and_functions = (
            ('h2[id^="a"]', self.parse_header_text),
            ('p img', self.parse_main_image),
            ('div[id^="gallery-"]', self.parse_product_images),
            ('table', self.parse_table),
        )
        for box in boxes:
            for key, val in identifiers_and_functions:
                if box.css(key):
                    try:
                        val(box, product_row, images)
                    except Exception as err:
                        print("=" * 20)
                        print("Error in parsing {} due to error: {}".format(key, err))
                        print("=" * 20)
        self.parse_pros_and_cons(response.css("div.ratings2"), product_row, images)
        # print(":" * 40)
        # print (product_row)
        # print(":" * 40)
        product_row.pop(self.header_key, None)
        product_row = {key: value for key, value in product_row.items() if key is not None}

        total_images = 0
        for key, value in images.items():
            total_images += len(value)

        if not (product_row or images):
            return
        product_row['Total Images'] = total_images
        data_points = product_row.pop(self.data_points_key, dict())
        item = UltrabookReviewItem()
        item['row'] = product_row
        item['images_urls'] = images
        item['data_points'] = data_points

        yield item

    def store_fetched_product_names(self):
        product_file = open(self.product_fetched_file_path, 'wb')
        data = {
            'products_fetched': self.products_fetched,
        }
        pickle.dump(data, product_file)
        product_file.close()

    def close(self, reason):
        if OUTPUT_DATA_TO_FILE:
            self.store_fetched_product_names()

        # Sending the email to source recipients
        # send_email_to_source_recipients(SOURCE, self.laptop_fetch_cnt)

        work_time = datetime.now() - self.started_on
        print("Total time spend in spider: {}".format(work_time))
