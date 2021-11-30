#!/bin/python3.7
from urllib.parse import urlparse
from async_crawler import AsyncCrawler
from csv_manager import CsvManager
from util import Util
from bs4 import BeautifulSoup
import logutil
import logging
import asyncio
import time
import ssl
import re
import os


_translate = {
    "brand": "marka",
    "model": "model",
    "production year": "rok produkcji",
    "fuel": "paliwo",
    # "euro class": "klasa euro",
    # "keys": "kluczyki",
    # "registration card": "dowód rejestracyjny"
    "mileage": "przebieg",
    "car body": "karoseria",
    "number of doors": "ilość drzwi",
    "equipment": "wyposażenie",
    "category": "kategoria",
    "title": "tytul",
    "auction": "aukcja",
    "images": "zdjecia",
    "parameters": "parametry",
    "attributes": "atrybuty",
    "description": "opis",
    "price": "cena",
    "engine": "silnik",
    "registration number": "nr rejestracyjny",
    "vehicle card": "Karta pojazdu",
    "finished": "zakończon"
}


log = logging.getLogger(__file__)
logutil.init_log(log, logging.DEBUG)


class IdeagetinCrawler(AsyncCrawler):

    def __init__(self, max_concurrency=200):
        AsyncCrawler.__init__(self, max_concurrency)

        self.site_url = "https://aukcje.ideagetin.pl"
        self.search_link_format = "https://aukcje.ideagetin.pl/aukcje/{category}/widok-lista/strona-{page_number}"

        self.output_dir_path_format = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "Ideagetin", "{category}")

        log.debug("Output directory path format: %s" % self.output_dir_path_format)

        # category name => catid
        self.categories = {
            "pojazdy-samochodowe-i-motocykle": 1,
            "maszyny-budowlane": 2,
            "przyczepy-naczepy": 3,
            "gastronomia-i-meble": 4,
            "maszyny-rolnicze-i-nbsp-lesne": 5,
            "medycyna-i-kosmetyka": 6,
            "wozki-widlowe": 7,
            "inne-maszyny-i-nbsp-urzadzenia": 8,
            "maszyny-produkcyjne": 9,
            "sport-i-rekreacja": 10
        }

        self.flags = {
            # 0. new record
            "new": 0,
            # 1. updated (new price, new date)
            "updated": 1,
            # 2. ended, sold (no renew)
            "sold": 2
        }

        self.fields = [
            ("id", str),
            ("link", str),
            ("category_id", int),
            ("category", str),
            ("title", str),
            ("start", str),
            ("stop", str),
            ("type", str),
            ("images", str),
            ("parameters", str),
            ("description", str),
            ("price", float),
            ("flag", int)
        ]

        self.field_names = [field_name for field_name, _ in self.fields]

    async def start(self):
        first_pages = [
            self.search_link_format.format(category=category, page_number=1)
            for category in self.categories.keys()
        ]
            
        cat_max_pages = list()

        tasks = (self.extract_async(url) for url in first_pages)
        for page in AsyncCrawler.limited_as_completed(tasks, 5):
            url, page_content = await page

            # Get max pages for this category
            max_page = 1
            soup = BeautifulSoup(page_content, 'html.parser')
            pagination = soup.find("div", {"class": "pagination"})
            if pagination is not None:

                for page_num in pagination.findAll("a"):
                    next_page_href = page_num.get("href").split('/')[-1]
                    if next_page_href is not None:
                        search = re.search("strona-([0-9]+)", next_page_href, re.IGNORECASE)
                        if search is not None:
                            max_page = max(max_page, int(search.group(1)))

            category = url.split('/')[-3]
            # await self.crawl_pages(category, max_page)
            cat_max_pages.append((category, max_page))

        tasks = (self.crawl_pages(cat, max_page) for cat, max_page in cat_max_pages)
        for page in AsyncCrawler.limited_as_completed(tasks):
            await page

    async def crawl_pages(self, category, max_pages):
        pages = (self.search_link_format.format(category=category, page_number=page_number)
                 for page_number in range(1, max_pages + 1))

        auctions_links = list()

        tasks = (self.extract_async(url) for url in pages)
        for page in AsyncCrawler.limited_as_completed(tasks, 5):
            url, page_content = await page
            if url is not None and page_content is not None:
                auctions_links.extend(self.parse_search_result_page(page_content))

        if not auctions_links:
            log.warning("No results found for category: %s" % category)
            return

        log.debug("Found: %d auctions in %d pages of category: %s" % (len(auctions_links), max_pages, category))
        
        output_dir = self.output_dir_path_format.format(category=category)
        csv_file_path = os.path.join(output_dir, "{category}.csv".format(category=category))

        Util.create_directory(output_dir)

        csv_manager = CsvManager(csv_file_path, self.fields, "id")
        csv_manager.open_file()

        '''
        tasks = (self.extract_multi_async([url.replace("aukcja", "zdjecia"), url]) for url in auctions_links)
        for pages in AsyncCrawler.limited_as_completed(tasks):
            results = await pages
            images_url, images_page_content = results[0]
            url, page_content = results[1]
        '''
        tasks = (self.extract_async(url) for url in auctions_links)
        for page in AsyncCrawler.limited_as_completed(tasks, 5):
            url, page_content = await page
            if url is not None and page_content is not None:
                extracted_data = self.parse_data(category, url, page_content)

                images_links = list()
                images_url = url.replace("aukcja", "zdjecia")
                _, images_page_content = await self.extract_async(images_url)
                if images_url is not None and images_page_content is not None:
                    images_links = self.parse_full_images_page(images_page_content)
                    extracted_data["images"] = '|'.join(images_links)

                if csv_manager.check_row_exist(extracted_data):
                    if _translate.get("finished") in extracted_data.get("stop").lower():
                        extracted_data["flag"] = self.flags.get("sold")
                    else:
                        extracted_data["flag"] = self.flags.get("updated")
                else:
                    extracted_data["flag"] = self.flags.get("new")

                csv_manager.update_row(extracted_data)
                # log.debug(extracted_data)

                auction_output_dir = os.path.join(output_dir, extracted_data.get("id"))
                # log.debug(auction_output_dir)
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

            else:
                logging.error("Url or page_content none: %s" % url)
        csv_manager.close_file()

    @staticmethod
    def get_image_id(img_url):
        search = re.search("/i/zd/zdjecie-([0-9]+)", img_url, re.IGNORECASE)
        if search is not None:
            return int(search.group(1))

    def parse_search_result_page(self, page_content):
        auctions_links = list()
        soup = BeautifulSoup(page_content, 'html.parser')
        l1 = soup.find("div", {"id": "listing-desktop"})
        # l2 = soup.find("div", {"id": "listing-mobile"})

        results = l1.findAll("div", {"class": "single-auction"})
        for r in results:
            left_pic_div = r.find("div", {"class": "left picture"})
            if left_pic_div is not None:
                res_uri = left_pic_div.find("a").get("href")
                auction_page_url = self.site_url + res_uri
                auctions_links.append(auction_page_url)

        return auctions_links

    def parse_full_images_page(self, page_content):
        full_images = list()
        images_soup = BeautifulSoup(page_content, 'html.parser')
        for a in images_soup.find("div", {"class": "bottom-images"}).findAll("a"):
            full_images.append(self.site_url + '/' + a.get("href"))
        return full_images

    def parse_data(self, category, page_url, page_content):
        auction_id = urlparse(page_url).path.split('/')[2]

        auction_soup = BeautifulSoup(page_content, 'html.parser')

        auction_content = auction_soup.find("div", {"class": "auction-content"})
        auction_content_top_bar = auction_soup.find("div", {"class": "left boxes"}).find("div", {"class": "top-bar"})

        auction_title = auction_content_top_bar.find("h1").text.strip()
        auction_info = auction_content_top_bar.find("div", {"class": "auction-information"}).find("p").text.strip()

        extracted_data = dict()
        extracted_data["id"] = auction_id
        extracted_data["link"] = page_url
        extracted_data["category_id"] = self.categories.get(category)
        extracted_data["category"] = category
        extracted_data["title"] = auction_title

        extracted_data["start"] = ""

        end_time = auction_soup.find("div", {"class": "auction-information"}).find("p").text.strip()
        extracted_data["stop"] = end_time
        extracted_data["type"] = "-"

        # images = list()
        # # Get the image
        # for img in auction_soup.findAll("img"):
        #     img_uri = img.get("data-lazy")
        #     if img_uri:
        #         img_url = self.site_url + img_uri
        #         images.append(img_url)
        # urllib.request.urlretrieve(img_url, os.path.join(dir_name, "img.jpg"))

        # full_images = list()
        # full_images_link = page_url.replace("aukcja", "zdjecia")
        # # print(full_images_link)
        # r = requests.get(url=full_images_link)
        # images_soup = BeautifulSoup(r.content, 'html.parser')
        # for a in images_soup.find("div", {"class": "bottom-images"}).findAll("a"):
        #     full_images.append(self.site_url + '/' + a.get("href"))

        # print('|'.join(full_images))
        # print('|'.join(images))

        # extracted_data["images"] = '|'.join(images)

        # brand|production year|mileage|engine|fuel|car_body|number of doors|registration number|vehicle card|equipment
        parameters = ["brand", "production year", "mileage", "engine", "fuel", "car body", "number of doors",
                      "registration number", "vehicle card", "equipment"]

        parameters_dict = dict()

        for data in auction_soup.findAll("div", {"class": "data-inner"}):
            for field in parameters:
                translate = _translate.get(field)
                if translate and translate in data.text.lower():
                    parameters_dict[translate] = data.text.lower().replace(translate + ":", '').strip()

        # Check equipment
        parameters_dict[_translate.get("equipment")] = list()
        equipment_div = auction_soup.find("div", {"class": "quartet"})
        if equipment_div is not None:
            for data in equipment_div.findAll("div", {"class": "data-inner"}):
                parameters_dict[_translate.get("equipment")].append(data.text.strip().lower())

        extracted_data["parameters"] = '|'.join([str(k) + ":" + str(v) for k, v in parameters_dict.items()])

        description = ''
        description_div = auction_soup.find("div", {"class": "full"})
        if description_div is not None:
            if description_div.find("p"):
                description = description_div.find("p").text.strip()

        extracted_data["description"] = description

        price_num = auction_soup.find("div", {"class": "price"}).find("span", {"class": "numbers"}).text.strip()
        span_currency = auction_soup.find("div", {"class": "price"}).find("span", {"class": "currency"})
        price_currency_smaller = span_currency.find("span", {"class": "smaller"}).text
        price_currency = span_currency.text.replace(price_currency_smaller, '')

        # extracted_data["price"] = "%s(%s) %s" % (price_num, price_currency, price_currency_smaller)
        extracted_data["price"] = Util.str_to_float(price_num)

        extracted_data["flag"] = 0
        return extracted_data


async def main():
    async with IdeagetinCrawler() as ideagetin_crawl:
        await ideagetin_crawl.start()
        #await ideagetin_crawl.extract_async("https://www.example.com")


if __name__ == '__main__':
    t0 = time.time()
    log.debug("Crawler started...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    log.debug("Took: %.2f seconds" % (time.time() - t0))
