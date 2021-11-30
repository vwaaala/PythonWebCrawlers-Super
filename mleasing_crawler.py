#!/bin/python3.7
from async_crawler import AsyncCrawler
from csv_manager import CsvManager
from util import Util
from datetime import datetime
from pytz import timezone
import logutil
import logging
import asyncio
import json
import time
import re
import os


log = logging.getLogger("mleasing_crawler")
logutil.init_log(log, logging.DEBUG)


class MleasingCrawler(AsyncCrawler):

    def __init__(self, max_concurrency=200):
        AsyncCrawler.__init__(self, max_concurrency)

        self.site_url = "https://portalaukcyjny.mleasing.pl/"
        self.offer_url_format = "https://portalaukcyjny.mleasing.pl/#/offer/{offer_id}/details"
        # self.search_category_url_format = \
        #     "https://portalaukcyjny.mleasing.pl/api/offer-read/search" \
        #     "?selectedCategory={cat_id}" \
        #     "&offerType%5B%5D=3" \
        #     "&$orderBy=AuctionType%20asc,IsPromoted%20desc,Id%20desc" \
        #     "&$skip={skip}"\
        #     "&$top={max_num_of_results}"

        self.search_category_url_format = \
            "https://portalaukcyjny.mleasing.pl/api/offer-read/search" \
            "?selectedCategory={cat_id}" \
            "&offerType%5B%5D=3" \
            "&$filter=(1%20eq%201)%20and%20((IsBuyNow%20eq%20true)%20or" \
            "%20(AuctionType%20eq%20Mleasing.SHL.Contracts.Enums.AuctionType%272%27))" \
            "&$orderBy=AuctionType%20asc,IsPromoted%20desc,Id%20desc" \
            "&$skip={skip}" \
            "&$top={max_num_of_results}"

        self.get_images_api_url_format = \
            "https://portalaukcyjny.mleasing.pl/api/offer-read/get-images?id={auction_id}"

        self.get_image_api_url_format = \
            "https://portalaukcyjny.mleasing.pl/api/offer-read/get-image?id={img_id}"

        self.output_dir_path_format = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "Mleasing", "{category}")

        log.debug("Output directory path format: %s" % self.output_dir_path_format)

        # category name => catid
        self.categories = {
            # "Wszystkie": 0,
            "Auto": 1,  # "Osobowe"
            "Vans": 2,  # "Dostawcze"
            "Lorry": 3,  # "Ciężarowe"
            "Devices": 4,  # "Urządzenia"
            "Medical": 5,  # "Medyczne"
            "Building": 6  # "Budowlane": 6,
            # "Other" # "Inne": 7
        }

        self.flags = {
            # 0. new record
            "new": 0,
            # 1. updated (new price, new date)
            "updated": 1,
            # 2. ended, sold (no renew)
            "sold": 2
        }

        self.types = {
            "auction": 1,
            "buynow": 2,
            "sell offer": 3
        }

        self.fields = [
            ("id", str),
            ("link", str),
            ("category_id", int),
            ("category", str),
            ("title", str),
            ("start", str),
            ("stop", str),
            ("type", int),
            ("images", str),
            ("parameters", str),
            ("description", str),
            ("price_pln", float),
            ("price_buy_now_pln", float),
            # 'mileage",
            ("flag", int)
        ]

        self.field_names = [field_name for field_name, _ in self.fields]

    async def start(self):
        tasks = (self.crawl_pages(category) for category in self.categories)
        for res in AsyncCrawler.limited_as_completed(tasks):
            await res

    async def crawl_pages(self, category):
        cat_id = self.categories.get(category)
        offset = 0
        max_results = 50
        auctions = list()

        while True:
            url = self.search_category_url_format.format(cat_id=cat_id, skip=offset, max_num_of_results=max_results)
            _, page_content = await self.extract_async(url)
            if page_content is not None:
                json_obj = json.loads(page_content.decode("utf-8"), encoding="utf-8")

                items = json_obj.get("Items")
                auctions.extend(items)

            offset += max_results

            if len(items) < max_results:
                break

        log.debug("Found: %d auctions of category: %s" % (len(auctions), category))

        output_dir = self.output_dir_path_format.format(category=category)
        csv_file_path = os.path.join(output_dir, "{category}.csv".format(category=category))

        log.info("Csv output directory path: %s, csv file: %s" % (output_dir, csv_file_path))

        Util.create_directory(output_dir)

        csv_manager = CsvManager(csv_file_path, self.fields, "id")
        csv_manager.open_file()

        tasks = (self.parse_item(category, item) for item in items)
        for res in AsyncCrawler.limited_as_completed(tasks, 5):
            extracted_data = await res

            if csv_manager.check_row_exist(extracted_data):
                extracted_data["flag"] = self.flags.get("updated")
            else:
                extracted_data["flag"] = self.flags.get("new")

            csv_manager.update_row(extracted_data)

            auction_output_dir = os.path.join(output_dir, extracted_data.get("id"))
            Util.create_directory(auction_output_dir)

            if extracted_data.get("images") is not None:
                images_urls = extracted_data.get("images").split('|')

                local_img = list()

                for img_url in images_urls:
                    local_img_file_path = os.path.join(
                        auction_output_dir,
                        "{img_id}.jpg".format(img_id=self.get_image_id(img_url)))

                    if not Util.check_file_exist(local_img_file_path):
                        local_img.append((img_url, local_img_file_path))

                download_tasks = (self.download_file(img_url, img_file_path)
                                  for img_url, img_file_path in local_img)

                for r in AsyncCrawler.limited_as_completed(download_tasks):
                    await r

        csv_manager.close_file()

    async def parse_item(self, category, item):
        extracted_data = dict()

        extracted_data["id"] = item.get("Id")
        extracted_data["link"] = self.offer_url_format.format(offer_id=item.get("Id"))
        extracted_data["category_id"] = self.categories.get(category)
        extracted_data["category"] = category
        extracted_data["title"] = item.get("Name")
        extracted_data["type"] = item.get("AuctionType")

        if extracted_data.get("type") == self.types.get("auction"):
            start_time_utc = item.get("From")
            extracted_data["start"] = None
            # replace +02:00 utc offset to +0200
            start_time_utc = start_time_utc[::-1].replace(':', '', 1)[::-1]
            if '.' in start_time_utc:
                start_datetime = datetime.strptime(start_time_utc, "%Y-%m-%dT%H:%M:%S.%f%z")
            else:
                start_datetime = datetime.strptime(start_time_utc, "%Y-%m-%dT%H:%M:%S%z")
            extracted_data["start"] = start_datetime.astimezone(timezone("Poland"))

            stop_time_utc = item.get("To")
            extracted_data["stop"] = None
            # replace +02:00 utc offset to +0200
            stop_time_utc = stop_time_utc[::-1].replace(':', '', 1)[::-1]
            if '.' in stop_time_utc:
                start_datetime = datetime.strptime(stop_time_utc, "%Y-%m-%dT%H:%M:%S.%f%z")
            else:
                stop_time_utc = datetime.strptime(stop_time_utc, "%Y-%m-%dT%H:%M:%S%z")
            extracted_data["stop"] = stop_time_utc.astimezone(timezone("Poland"))

            extracted_data["price_pln"] = item.get("Amount")
            extracted_data["price_buy_now_pln"] = item.get("AmountBuyNow")

        # elif extracted_data.get("type") == self.types.get("buynow"):
        #     print("Buy now: ", extracted_data.get("link"), extracted_data.get("title"), item)

        extracted_data["description"] = item.get("Description")

        parameters = ["Make", "Model", "Type", "RegistrationNumber", "Engine power", "Mileage",
                      "Date of first registration", "GearBoxType", "FuelType", "engine capacity", "ProductionYear"]

        parameters_dict = dict()
        for param in parameters:
            if item.get(param) is not None:
                parameters_dict[param] = item.get(param)

        extracted_data["parameters"] = '|'.join([str(k) + ":" + str(v) for k, v in parameters_dict.items()])

        images = list()
        _, page_content = await self.extract_async(self.get_images_api_url_format.format(auction_id=item.get("Id")))
        img_json_obj = json.loads(page_content.decode("utf-8"), encoding="utf-8")
        for img_json in img_json_obj:
            images.append(self.get_image_api_url_format.format(img_id=img_json.get("Id")))

        extracted_data["images"] = '|'.join(images)

        return extracted_data

    @staticmethod
    def get_image_id(image_url):
        search = re.search("id=([0-9]+)", image_url, re.IGNORECASE)
        if search is not None:
            return int(search.group(1))


async def main():
    async with MleasingCrawler() as mleasing_crawler:
        await mleasing_crawler.start()


if __name__ == '__main__':
    t0 = time.time()
    log.debug("Crawler started...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    log.debug("Took: %.2f seconds" % (time.time() - t0))
