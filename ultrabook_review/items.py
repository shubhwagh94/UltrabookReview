# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class UltrabookReviewItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    row = scrapy.Field()
    images_urls = scrapy.Field()
    data_points = scrapy.Field()
