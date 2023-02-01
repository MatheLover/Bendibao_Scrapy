import sys

import scrapy
from bs4 import BeautifulSoup
import pandas as pd


class BendibaoSpider(scrapy.Spider):
    # identifies the Spider (unique)
    name = "Bendibao"
    # base url for news article
    url_1 = 'http://m.bendibao.com'

    # base url for next page
    url_3 = 'http://m.bendibao.com'

    def start_requests(self):
        """"
        Return a list of requests from which the Spider will crawl from
        """

        # first url to start
        urls = [
            'http://m.bendibao.com/news/list.php?sid=17&cid=1720'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """
        Get article urls in the first result page, parse them to article_parse methods, and obtain next result page urls
        :param response: an instance of TextResponse holding the first page content
        :return:
        """

        # url list for all articles on current page
        # shenzhen should div.sec-list-body
        # other places div.list-item2016
        url_list_articles = response.css('div.sec-list-body a::attr(href)').extract()

        # construct full urls for each article
        full_url_list = []
        for url in url_list_articles:
            full_url_list.append(self.url_1+url)

        # process article urls current page
        for article_url in full_url_list:
            yield scrapy.Request(url=article_url,
                                 callback=self.parse_article)

        # find url for next result page
        try:
            next_page_url = response.xpath("//a[text()='>']").css("::attr(href)").get()
            if next_page_url:
                # construct full next page url
                full_next_page_url = self.url_3 + next_page_url
                # yield request and recursively call parse methods
                yield scrapy.Request(url=full_next_page_url,
                                 callback=self.parse)
        except:
            print('No more pages')

    def parse_article(self, response):
        result = BeautifulSoup(response.text, "html.parser")

        # article title
        title = result.find("h1").get_text()

        # article time
        article_time = result.find("span", class_="public_time").get_text()
        year = article_time[0:4]

        # if year is less than 2020, exit
        if int(year) < 2020:
            return

        # article texts
        article_content = result.find_all("div", class_="content-box")
        article_text = ""
        # find all p tags
        p_tag_list = article_content[0].find_all("p")
        for d in p_tag_list:
            if d.find('table') is None:
                article_text += (d.text.strip())

        # article table
        table = result.find('table')
        if table is not None:
            rows = None
            if table is not None:
                rows = table.find_all('tr')
            if rows is not None:
                first = rows[0]
                allRows = rows[1:-1]
                escapes = ''.join([chr(char) for char in range(1, 32)])
                translator = str.maketrans('', '', escapes)
                headers = [header.get_text().strip() for header in first.find_all('td')]
                test = [[data.get_text().translate(translator) for data in row.find_all('td')] for row in allRows]

                rowspan = []

                for no, tr in enumerate(allRows):
                    for td_no, data in enumerate(tr.find_all('td')):
                        if data.get("rowspan") is not None:
                            t = data.get_text()
                            escapes = ''.join([chr(char) for char in range(1, 32)])
                            translator = str.maketrans('', '', escapes)
                            t = t.translate(translator)
                            rowspan.append((no, td_no, int(data["rowspan"]), t))
                if rowspan:
                    for i in rowspan:
                        # i[0], i[1], i[2], i[3] -- row index involving repetitive data (non-header rows), row index td with row span,number of repetitions  ,repetitive data
                        # tr value of rowspan in present in 1th place in results
                        for j in range(1, i[2]):
                            # - Add value in next tr.
                            test[i[0] + j].insert(i[1], i[3])

            # create df for tables
            try:
                df = pd.DataFrame(data=test, columns=headers)
                df = df.to_string()
            except:
                df = ""

        # concatenate articles and tables
        if 'df' in locals() and len(df) != 0:
            article_text = article_text + '\n\n' + df

        yield {'Time':article_time,
               'Title':title,
               'Content':article_text}



