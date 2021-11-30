#!/bin/python3.7
from async_crawler import AsyncCrawler
from csv_manager import CsvManager
from util import Util
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logutil
import logging
import asyncio
import time
import sys
import re
import os


_translate = {
    "brand": ["marka", "producent"],
    "model": ["model", "model pojazdu"],
    "production year": ["rok produkcji", "rocznik"],
    "version": ["typ/wersja", "wersja"],
    "chasis": ["nadwozie", "rodzaj nadwozia", "karoseria"],  # "typ",
    "doors": ["liczba drzwi", "drzwi", "ilość drzwi"],
    "seats": ["liczba miejsc"],
    "fuel": ["paliwo", "rodzaj paliwa"],
    # "diesel": ["diesel", "olej napędowy"],
    # "petrol": "benzyna",
    # "hybrid": ["hybryda", "napęd hybrydowy"],
    # "euro class": "klasa euro",
    # "keys": "kluczyki",
    "registration card": "dowód rejestracyjny",
    "date of 1st registration": ["Data 1. rejestracji"],
    "date of first registration": ["Data 1. rejestracji"],
    "engine size": ["Pojemność silnika", "pojemność"],
    "engine power": ["Moc silnika", "moc"],
    "transmission": ["skrzynia", "skrzynia biegów", "Rodzaj skrzyni biegów"],
    "milleage": ["Stan licznika", "licznik", "przebieg", "przebieg odczytany", "Wskazanie drogomierza"],
    "price": ["cena", "cena wywoławcza"],
    "registration number": ["numer rejestracyjny", "nr rejestracyjny"],
    "color": ["kolor", "Kolor powłoki lakierowej", "barwa", "barwa lakieru", "kolor lakieru"],
    "options": "wyposażenie",
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
    "engine": "silnik",
    "vehicle card": "Karta pojazdu",
    "finished": "zakończon"
}

log = logging.getLogger("pkoleasing_crawler")
logutil.init_log(log, logging.DEBUG)


class PkoleasingCrawler(AsyncCrawler):

    def __init__(self, max_concurrency=200):
        AsyncCrawler.__init__(self, max_concurrency)

        self.site_url = "https://aukcje.pkoleasing.pl/en/"
        self.search_category_url_format = \
            "https://aukcje.pkoleasing.pl/en/auctions/list/pub/all/{category}/all?page={page_number}"

        self.output_dir_path_format = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                "Pkoleasing", "{category}")

        log.debug("Output directory path format: %s" % self.output_dir_path_format)

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        self.driver = webdriver.Chrome(executable_path="/bin/chromedriver", options=chrome_options)

        # category name => catid
        self.categories = {
            "vehicles": 1,
            "ecr_machinery": 2,
            "ecr_agricultural": 3,
            "ecr_industrial": 4,
            "ecr_medic": 5,
            "ecr_trailers1": 6,
            "ecr_bus": 7,
            "ecr_motorcycles": 8,
            "ecr_other1": 9
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
            ("price_pln", float),
            ("price_buy_now_pln", float),
            ("price_pln_brutto", float),
            ("price_euro", float),
            # 'mileage",
            ("flag", int)
        ]

        self.field_names = [field_name for field_name, _ in self.fields]

    async def start(self):
        first_pages = [
            self.search_category_url_format.format(category=category, page_number=1)
            for category in self.categories.keys()
        ]

        cat_max_pages = list()

        tasks = (self.extract_async(url) for url in first_pages)
        for page in AsyncCrawler.limited_as_completed(tasks, 5):
            url, page_content = await page
            # Get max pages for this category
            max_page = 1
            soup = BeautifulSoup(page_content, 'html.parser')
            pagination = soup.find("ul", {"class": "pagination"})
            if pagination is not None:
                for page_num in pagination.findAll("a"):
                    next_page_href = page_num.get("href")
                    if next_page_href is not None:
                        search = re.search("page=([0-9]+)", next_page_href, re.IGNORECASE)
                        if search is not None:
                            max_page = max(max_page, int(search.group(1)))

            category = url.split('/')[-2]
            # await self.crawl_pages(category, max_page)
            cat_max_pages.append((category, max_page))

        for cat, max_page in cat_max_pages:
            log.debug("Category: %s -> paged %d" % (cat, max_page))

        tasks = (self.crawl_pages(cat, max_page) for cat, max_page in cat_max_pages)
        for page in AsyncCrawler.limited_as_completed(tasks):
            await page

    async def crawl_pages(self, category, max_pages):
        pages = (self.search_category_url_format.format(category=category, page_number=page_number)
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

        for auction_url in auctions_links:
            self.driver.get(auction_url)

            extracted_data = self.parse_data(category, auction_url, self.driver.page_source)
            if csv_manager.check_row_exist(extracted_data):
                log.debug("row already existed in csv")
                extracted_data["flag"] = self.flags.get("updated")
            else:
                log.debug("row in new")
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
                        "{img_id}.png".format(img_id=self.get_image_id(img_url)))

                    if not Util.check_file_exist(local_img_file_path):
                        local_img.append((img_url, local_img_file_path))

                download_tasks = (self.download_file(img_url, img_file_path)
                                  for img_url, img_file_path in local_img)

                for r in AsyncCrawler.limited_as_completed(download_tasks):
                    await r

        csv_manager.close_file()

    @staticmethod
    def get_image_id(img_url):
        search = re.search("oryginal_([a-z0-9-]+)", img_url, re.IGNORECASE)
        if search is not None:
            return search.group(1)

    @staticmethod
    def parse_search_result_page(page_content):
        auctions_links = list()
        soup = BeautifulSoup(page_content, 'html.parser')

        results = soup.findAll("div", {"class": "list-item"})
        if not results:
            log.warning("No results found")
        else:
            for r in results:
                anchor_url = r.find("a")
                if anchor_url is not None:
                    auctions_links.append(anchor_url.get("href"))

        return auctions_links

    def parse_pdf_url(self, page_content):
        soup = BeautifulSoup(page_content, 'html.parser')
        auction_soup = soup.find("div", {"class": "auction"})

        if auction_soup is None:
            log.error("Auction soup is None")
            return None

        pdf_anchor = auction_soup.find("a", {"class": "ico-pdf"})
        if pdf_anchor is not None:
            pdf_url = pdf_anchor.get("href")
            return pdf_url
        return None

    def parse_data(self, category, page_url, page_content):
        soup = BeautifulSoup(page_content, 'html.parser')

        auction_soup = soup.find("div", {"class": "auction"})

        if auction_soup is None:
            log.error("Auction soup is None")
            return None

        extracted_data = dict()
        extracted_data["link"] = page_url

        auction_id = page_url.split('/')[-1]
        extracted_data["id"] = auction_id
        extracted_data["link"] = page_url
        extracted_data["category_id"] = self.categories.get(category)
        extracted_data["category"] = category

        auction_title = ""
        title = auction_soup.find("div", {"class": "col-md-6"})
        if title is not None:
            if title.find("h1"):
                auction_title = title.find("h1").text.strip()
        extracted_data["title"] = auction_title

        auction_end_date = ""
        end_date = auction_soup.find("span", {"ng-bind": "endDate"})
        if end_date is not None:
            auction_end_date = end_date.text
            extracted_data["stop"] = auction_end_date

        extracted_data["stop"] = auction_end_date
        extracted_data["start"] = ""

        auction_buy_now_price = None
        buy_now_price = auction_soup.find("span", {"class": ["price", "value"], "style": "font-size: 18px;"})
        if buy_now_price is not None:
            auction_buy_now_price = buy_now_price.text
        if auction_buy_now_price is not None:
            auction_buy_now_price = Util.str_to_float(auction_buy_now_price.replace("PLN", ""))
        else:
            print(page_url, " auction buy now price is none")

        auction_current_price_pln = None
        current_price = auction_soup.find("span", {"ng-bind": "current_price"})
        if current_price is not None:
            auction_current_price_pln = current_price.text
        if auction_current_price_pln is not None:
            auction_current_price_pln = Util.str_to_float(auction_current_price_pln)

        auction_current_price_pln_brutto = None
        current_price_pln_brutto = auction_soup.find("span", {"ng-bind": "current_price_brutto"})
        if current_price_pln_brutto is not None:
            auction_current_price_pln_brutto = current_price_pln_brutto.text
        if auction_current_price_pln_brutto is not None:
            auction_current_price_pln_brutto = Util.str_to_float(auction_current_price_pln_brutto)

        auction_current_price_euro = ""
        current_price_euro = auction_soup.find("span", {"ng-bind": "current_price_euro"})
        if current_price_euro is not None:
            auction_current_price_euro = current_price_euro.text
        if auction_current_price_euro is not None:
            auction_current_price_euro = Util.str_to_float(auction_current_price_euro)

        # log.debug("Current price (%.2f) PLN" % auction_current_price_pln)
        # log.debug("Current price (%.2f) PLN BRUTTO" % auction_current_price_pln_brutto)
        # log.debug("Current price (%.2f) EURO" % auction_current_price_euro)
        # log.debug("BUY NOW PLN: (%.2f) PLN" % auction_buy_now_price)

        extracted_data["price_pln"] = auction_current_price_pln
        extracted_data["price_pln_brutto"] = auction_current_price_pln_brutto
        extracted_data["price_euro"] = auction_current_price_euro
        extracted_data["price_buy_now_pln"] = auction_buy_now_price

        # extracted_data["price"] = "%s (PLN) %s (PLN BRUTTO) %s EURO, %s BUY NOW (PLN)" % (
        #     auction_current_price_pln,
        #     auction_current_price_pln_brutto,
        #     auction_current_price_euro,
        #     auction_buy_now_price
        # )

        parameters = ["Type of sale", "Registration number", "Type", "Engine power", "Mileage",
                      "Date of first registration", "Gearbox", "Fuel", "Color", "VIN", "engine capacity"]

        parameters_dict = dict()

        elements = auction_soup.findAll("div", {"class": ["row", "font-16", "mt-4"]})
        if elements is not None:
            for element in elements:
                for e in element.findAll("div", {"class": ["col-12", "col-md-4", "col-lg-3", "mb-2"]}):
                    spans = e.findAll("span")
                    if spans is not None:
                        for info in parameters:
                            for span in spans:
                                if info == span.text.strip():
                                    parameters_dict[info] = spans[1].text.strip()

        extracted_data["parameters"] = '|'.join([str(k) + ":" + str(v) for k, v in parameters_dict.items()])

        images = list()
        # Get the image
        for img in auction_soup.findAll("img", {"class": "img-fluid"}):
            img_url = img.get("src")
            img_url_original = img_url.replace("middle", "oryginal")
            images.append(img_url_original)

        extracted_data["images"] = '|'.join(images)

        # elements = auction_soup.findAll("div", {"class": "col-12"})
        # if elements is not None:
        #     for element in elements:
        #         if element.find("h2") and element.find("h2").text == "Aditional description":
        #             print(element.find("h2").text)

        return extracted_data


async def main():
    async with PkoleasingCrawler() as pkoleasing_crawler:
        await pkoleasing_crawler.start()


if __name__ == '__main__':
    t0 = time.time()
    log.debug("Crawler started...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    log.debug("Took: %.2f seconds" % (time.time() - t0))
