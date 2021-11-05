# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import pickle
from datetime import datetime
from urllib.parse import urlparse

import pymongo as pymongo
import scrapy
import xlrd
from itemadapter import ItemAdapter
from scrapy.pipelines.images import ImagesPipeline
from xlutils.copy import copy
from xlwt import Workbook

from ultrabook_review.settings import (IMAGES_STORE, MONGODB_SERVER,
                                       MONGODB_COLLECTION,
                                       MONGODB_PORT, MONGODB_DB)


class UltrabookReviewPipeline:
    col_index = 0
    sheet = None
    row_index = 1
    writer = None
    headers = dict()
    output_dir = 'output'
    headers_data_file_name = '.header_cols.pkl'
    output_file_name = 'output.xls'
    output_file_path = os.path.join(output_dir, output_file_name)
    headers_data_file_path = os.path.join(output_dir, headers_data_file_name)
    data_points_dir = 'data_points'

    def open_spider(self, _):
        # Create directory if not exists
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)
        try:
            data = pickle.load(open(self.headers_data_file_path, 'rb')) or set()
            self.headers = data['headers']
        except (FileNotFoundError, KeyError):
            pass
        except Exception as err:
            print("Unable to load the fetched products list due to error: {}".format(err))
        flag = os.path.exists(self.output_file_path)
        if not flag:
            self.writer = Workbook()
            self.sheet = self.writer.add_sheet('data')
            self.row_index = 1
            self.col_index = 0
        else:
            sheet_wb = xlrd.open_workbook(self.output_file_path)
            sheet_rd = sheet_wb.sheet_by_index(0)
            self.row_index = sheet_rd.nrows
            self.col_index = sheet_rd.ncols
            self.writer = copy(sheet_wb)
            self.sheet = self.writer.get_sheet(0)

    def add_column(self, column_name):
        self.sheet.write(0, self.col_index, column_name)
        self.headers[column_name] = self.col_index
        self.col_index += 1

    def add_sub_column(self, col, sub_col):
        ind, child = self.headers[col]
        self.sheet.write(1, ind + child, sub_col)
        self.headers[(col, sub_col)] = ind + child
        self.sheet.merge(0, 0, ind, ind + child)
        self.headers[col] = ind, child + 1
        self.col_index += 1

    def write_row(self, row, ):
        row_data = dict()
        for key, val in row.items():
            if key is None:
                continue
            if type(val) is dict:
                for sub_key, sub_val in val.items():
                    final_key = '{}_{}'.format(key, sub_key)
                    if final_key not in self.headers:
                        self.add_column(final_key)
                    row_data[self.headers[final_key]] = sub_val
                    # self.sheet.write(self.row_index, self.headers[final_key], sub_val)
            else:
                if key not in self.headers:
                    self.add_column(key)
                row_data[self.headers[key]] = val
                # self.sheet.write(self.row_index, self.headers[key], val)
        for col, val in row_data.items():
            if type(val) is str:
                val = val.strip()
            self.sheet.write(self.row_index, col, val)
        self.row_index += 1

    def save_data_points(self, item):
        row = item['row']
        data_points = item['data_points'] or dict()
        if not data_points:
            return
        store_directory = "{}".format(row['Product Name'])
        store_directory = store_directory.replace("/", "-")
        data_points_dir = os.path.join(self.output_dir, self.data_points_dir, store_directory)
        if not os.path.exists(data_points_dir):
            os.makedirs(data_points_dir)
        for key, points in data_points.items():
            file_name = os.path.join(data_points_dir, key + '.txt')
            with open(file_name, 'w') as f:
                f.write(points)

    def process_item(self, item, _):
        self.save_data_points(item)
        self.write_row(item['row'])
        return item

    def close_spider(self, _):
        product_file = open(self.headers_data_file_path, 'wb')
        data = {
            'headers': self.headers
        }
        pickle.dump(data, product_file)
        product_file.close()
        self.writer.save(self.output_file_path)


class MongoDBPipeline:
    def __init__(self):
        self.connection = pymongo.MongoClient(
            MONGODB_SERVER,
            MONGODB_PORT
        )
        db = self.connection[MONGODB_DB]
        self.products = db[MONGODB_COLLECTION]
        self.new_products_count = 0

    def process_item(self, item, _):
        data = dict(item['row'])
        data['fetched_at_timestamp'] = datetime.now()
        self.products.insert(data, check_keys=False)
        print("Stored {} in {} DB".format(data["Source"], "UltrabookReview"))
        self.new_products_count += 1
        return item

    def close_spider(self, _):
        self.connection.close()


class DataUnitConversionPipeline:
    unit_types = dict()
    unit_types_file_name = 'unit-types.txt'
    header_mappings = dict()
    output_dir = 'output'
    headers_data_file_name = '.header_mapping.pkl'
    headers_data_file_path = os.path.join(output_dir, headers_data_file_name)

    def parse_unit_types(self):
        if not os.path.exists(self.unit_types_file_name):
            return
        f = open(self.unit_types_file_name)
        for line in f:
            typ, typ_name = line.strip().split('-')
            self.unit_types[typ] = typ_name

    def open_spider(self, _):
        self.parse_unit_types()
        try:
            data = pickle.load(open(self.headers_data_file_path, 'rb')) or set()
            self.header_mappings = data['header_mappings']
        except (FileNotFoundError, KeyError):
            pass
        except Exception as err:
            print("Unable to load the fetched products list due to error: {}".format(err))

    def close_spider(self, _):
        product_file = open(self.headers_data_file_path, 'wb')
        data = {
            'header_mappings': self.header_mappings
        }
        pickle.dump(data, product_file)
        product_file.close()

    def process_item(self, item, _):
        adapter = ItemAdapter(item)
        row = adapter.get('row')
        new_row = dict()
        for key, val in row.items():
            val = str(val).strip()
            if key not in self.header_mappings:
                new_key = key
                types = list()
                for typ in self.unit_types:
                    if typ in val:
                        new_key += "({})".format(self.unit_types[typ])
                        types.append(typ)
                self.header_mappings[key] = new_key, types
            else:
                new_key, types = self.header_mappings[key]

            new_val = val
            for typ in types:
                new_val = new_val.replace(typ, '')
            try:
                new_val = float(new_val.replace(',', ''))
                if int(new_val) == new_val:
                    new_val = int(new_val)
            except (ValueError, AttributeError):
                pass
            new_row[new_key] = new_val
        adapter['row'] = new_row
        return item


class LaptopImagesPipeline(ImagesPipeline):
    images_directory = 'images'

    def get_media_requests(self, item, info):
        row = item['row']
        store_directory = "{}".format(row['Product Name'])
        store_directory = store_directory.replace("/", "-")
        for image_key, image_urls in item['images_urls'].items():
            for image_url, image_text in image_urls:
                yield scrapy.Request(image_url, meta={'store_directory': store_directory,
                                                      'image_key': image_key,
                                                      'image_text': image_text})

    def file_path(self, request, response=None, info=None):
        store_directory = request.meta.get('store_directory')
        image_key = request.meta.get('image_key')
        image_text = request.meta.get('image_text')
        path = os.path.join(self.images_directory,
                            store_directory,
                            image_key,
                            os.path.basename(urlparse(request.url).path)
                            )
        directory_path = os.path.join(IMAGES_STORE, self.images_directory, store_directory,
                                      image_key)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        if image_text:
            image_filename, ext = os.path.basename(urlparse(request.url).path).split('.')
            with open(os.path.join(directory_path, image_filename + '.txt'), 'w') as f:
                f.write(image_text)
        return path
