"""A crawler to collect ISNA news."""
import io
import os
from datetime import timedelta, datetime

import re
import sys

from django.utils import timezone, dateparse
from khayyam import JalaliDatetime, JalaliDate

import django
import scrapy

from crawler.items import NewsItem
from shimc_webapp import models

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
os.environ['DJANGO_SETTINGS_MODULE'] = 'shimc.settings'
django.setup()
from django.db.utils import IntegrityError


class ISNASpider(scrapy.Spider):
    name = "ISNA"

    def __init__(self, date_from=None, date_to=None, *args, **kwargs):
        super(ISNASpider, self).__init__(*args, **kwargs)
        self.date_from = date_from
        self.date_to = date_to
        self.date_news_finished = False
        self.dates = self.get_dates()
        self.last_crawled_date = None

    @classmethod
    def is_date_news(cls, date, news_date):
        """"Check to see if the date of news is equal to a given date."""
        year_condition = date.year == news_date.year
        month_condition = date.month == news_date.month
        day_condition = date.day == news_date.day
        return year_condition and month_condition and day_condition

    @classmethod
    def get_genre(cls, genre_id):
        return {
            '14': models.GENRE_POLITICS,
            '17': models.GENRE_POLITICS,  # بین‌الملل
            '34': models.GENRE_ECONOMY,
            '9': models.GENRE_SOCIAL,
            '20': models.GENRE_CULTURE,
            '5': models.GENRE_SCIENCE,
            '24': models.GENRE_SPORTS
        }[str(genre_id)]

    def get_dates(self):
        """Get dates between date_from and date_to"""
        if self.date_from and self.date_to:
            date_from = JalaliDate(int(self.date_from[:4]),
                                   int(self.date_from[4:6]),
                                   int(self.date_from[6:]))
            date_to = JalaliDate(int(self.date_to[:4]),
                                 int(self.date_to[4:6]),
                                 int(self.date_to[6:]))
            date_counter = date_from
            dates = []
            while date_counter <= date_to:
                dates.append(date_counter)
                date_counter += timedelta(days=1)
            return dates
        else:
            # Get yesterday persian date
            now = timezone.make_aware(
                datetime.now(), timezone.get_default_timezone())
            yesterday = JalaliDate(now) - timedelta(days=1)
            return [yesterday]

    def start_requests(self):
        last_date = self.dates[-1]
        self.last_crawled_date = last_date
        url_placeholder = ('http://www.isna.ir/archive?pi={page_id}&ms=0&dy=' +
                           str(last_date.day) + '&mn=' + str(last_date.month) +
                           '&yr=' + str(last_date.year))

        yield scrapy.Request(url=url_placeholder.format(page_id=1),
                             callback=self.parse,
                             meta={"url_placeholder": url_placeholder, "page_id": 1})

    def parse(self, response):
        # Check to see if page is not empty
        # And If there is not more news, go to the date after last crawled date
        items = response.xpath(
            '//div[@class="page itemlist"]//div[@class="items"]').extract()
        if not items:
            last_date = self.last_crawled_date - timedelta(days=1)
            url_placeholder = ('http://www.isna.ir/archive?pi={page_id}&ms=0&dy=' +
                               str(last_date.day) + '&mn=' + str(last_date.month) +
                               '&yr=' + str(last_date.year))
            yield scrapy.Request(url=url_placeholder.format(page_id=1),
                                 callback=self.parse,
                                 meta={"url_placeholder": url_placeholder, "page_id": 1})
            return

        # Extract news links from page
        news_links = response.xpath(
            "//div[@class='items']/ul/li/div[@class='desc']/h3/a/@href").extract()

        # Extract short news url
        news_links = [re.match(r"/news/(\d+)/", link) for link in news_links]
        news_links = [{"url": match.group(0),
                       "code": match.group(1)} for match in news_links if match is not None]

        print("-" * 100)
        print("Crawling archive page {0}".format(response.meta.get("page_id")))
        print("Last crawled date: " + str(self.last_crawled_date))
        print("-" * 100)

        for link in news_links:
            news_code = link["code"]
            url = 'http://www.isna.ir' + link["url"]

            yield scrapy.Request(url=url,
                                 callback=self.parse_news,
                                 meta={"news_code": news_code, "news_url": url})

        # If we have news for given dates in next pages of archives, go to next pages
        if not self.date_news_finished:
            url = response.meta.get("url_placeholder").format(
                page_id=response.meta.get("page_id") + 1
            )
            yield scrapy.Request(url=url, callback=self.parse,
                                 meta={"url_placeholder": response.meta.get("url_placeholder"),
                                       "page_id": response.meta.get("page_id") + 1})

    def parse_news(self, response):
        """Parse each individual news"""
        news_code = response.meta.get("news_code")

        published_date = response.xpath('//div[@class="meta-news"]//' +
                                        'meta[@itemprop="datePublished"]/@content').extract_first()

        published_date = dateparse.parse_datetime(published_date)

        published_date_jalali = JalaliDate(published_date)

        # news_date = response.xpath('//div[@class="news-info"]/div' +
        #                            '[@class="meta-news"]/ul/li[1]/span' +
        #                            '[@class="text-meta"]/text()').extract_first().strip()
        # news_date = JalaliDatetime.strptime(news_date, '%D %B %N / %h:%r')

        if published_date_jalali not in self.dates:
            self.date_news_finished = True
            return

        self.last_crawled_date = published_date_jalali

        # save the entire response in html files
        with io.open(file=os.path.join(os.path.dirname(__file__),
                                       "FILES_ISNA/" + news_code + ".html"),
                     mode="w", encoding="utf-8") as response_file:
            response_file.write(response.body.decode("utf-8"))

        title = response.xpath(
            '//div[@class="full-news-text"]//h1[@class="first-title"]/text()').extract_first()
        text = response.xpath(
            '//div[@class="full-news-text"]//div[@class="item-text"]//text()').extract()
        text = ''.join(text)
        summary = response.xpath('//div[@class="full-news-text"]//' +
                                 'p[@class="summary"]//text()').extract_first()

        reporter_code = response.xpath('//div[@class="meta-news"]//' +
                                       'span[@class="text-meta"]/text()').extract()[3].strip()
        link = response.meta.get("news_url")
        sub_genre = response.xpath('//div[@class="news-info"]/div[@class="meta-news"]' +
                                   '/ul/li[2]/span[@class="text-meta"]' +
                                   '/text()').extract_first().strip()

        genre = response.xpath(
            '//div[@class="service-title"]//a/@href').extract_first()
        genre = genre.split('/')[-1][:3].upper()
        # if response.meta.get("genre_id") == 17:

        if genre == "WOR":  # WORLD
            genre = models.GENRE_POLITICS
            sub_genre = "بین‌الملل: " + sub_genre

        if genre == "MAR" and sub_genre == 'خبر بازار':  # MARKET
            genre = models.GENRE_ECONOMY
            sub_genre = "بازار: " + sub_genre

        possible_genres = [models.GENRE_POLITICS,
                           models.GENRE_POLITICS,
                           models.GENRE_ECONOMY,
                           models.GENRE_SOCIAL,
                           models.GENRE_CULTURE,
                           models.GENRE_SCIENCE,
                           models.GENRE_SPORTS]

        if genre not in possible_genres:
            return

        news = NewsItem()
        news["agency"] = models.AGENCY_ISNA
        news["news_code"] = news_code
        news["text"] = text
        news["summary"] = summary if summary else ''
        news["date"] = timezone.make_aware(
            published_date, timezone.get_default_timezone())
        # news["genre"] = ISNASpider.get_genre(response.meta.get("genre_id"))
        news["genre"] = genre
        news["title"] = title[:256]
        news["author"] = reporter_code
        news["link"] = link
        news["sub_genre"] = sub_genre[:32]

        try:
            news.save()
        except IntegrityError as err:
            # The news with this agency and news_code already exists in db.
            print(err)
