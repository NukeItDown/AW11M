from parsing import Parser, HtmlTraversal
from query import Result
from bs4 import BeautifulSoup
from urllib.parse import urlencode


class AtehnoParser(Parser):
    def __init__(self):
        super().__init__("atehno", "atehno.md", "/search/catalog", "/page", "category")

    def get_markup(self, term, page):
        #url = "https://{0}{1}&{2}".format(self.host, self.path, query)

        page = self.driver.get("https://{0}{1}/page/{2}?category={3}".format(self.host, self.path, page, term))
        html = self.driver.page_source
        soup = BeautifulSoup(html)
        body = soup.find('body')

        return body

    def extract_page_count(self, markup):
        count = 1
        return 1
        traverse = HtmlTraversal(markup)
        elements = traverse.get_elements("ul", {"class": "pagination pagination-centered"})
        traverse2 = HtmlTraversal(elements[0])
        elements = traverse2.get_elements("li", {})

        # if len(elements) > 0:
        #     count = int(traverse.in_element(elements[-1]).get_value())

        return count

    def extract_results(self, markup):
        results = []

        elements = markup.findAll("div", {"class": "row"})

        for x in elements:
            result = Result()
            result.title = x.find("h3", {}).text.strip()
            result.url = x.find("a", {}, "href").text.strip()
            result.price = x.find("span", {"class": "amount"}).text.strip()
            results.append(result)

        return results