from parsing import Parser, HtmlTraversal
from query import Result
from bs4 import BeautifulSoup


class EbayParser(Parser):
    def __init__(self):
        super().__init__("ebay", "www.ebay.com", "/sch/i.html?_from=R40&_trksid=p2380057.m570.l1313", "_nkw", "_pgn")

    def extract_page_count(self, markup):
        count = 1

        traverse = HtmlTraversal(markup)
        elements = traverse.get_elements("a", {"class": "pagination__item"})

        if len(elements) > 0:
            count = int(traverse.in_element(elements[-1]).get_value())

        return count

    def extract_results(self, markup):
        results = []

        elements = markup.findAll("li", {"class": "s-item"})

        for x in elements:
            result = Result()
            result.title = x.find("a", {"class": "s-item__link"}).text.strip()
            result.url = x.find("a", {"class": "s-item__link"}, "href").text.strip()
            result.price = x.find("span", {"class": "s-item__price"}).text.strip()
            results.append(result)

        return results