from otodom.otodom_jobs import individual_offer_scan
from otodom.otodom_offer_parser import offer_parse_parameters
from otodom_extractor import extract_page_number, extract_links_from_url, extract_parameters, extract_price_district, \
    prepare_links


def extract_page_num_test(url):
    extract_page_number(url)


def extract_links_from_url_test(url):
    extract_links_from_url(url)


def extract_parameters_test(url):
    extract_parameters(url)


def extract_price_district_test(url):
    extract_price_district(url)


def check_params():
    links = prepare_links(
        'https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/dolnoslaskie/wroclaw/wroclaw/wroclaw?viewType=listing', 5)

    urls = []
    params_uniqe = set()
    for link in links:
        urls.append(extract_links_from_url(link))

    for url in urls:
        for address in url:
            params = extract_parameters(address)
            params_uniqe.update([t[0] for t in params])
        a = 3


def dry_run_params(url):
    url_params = extract_parameters(url)
    params = offer_parse_parameters(url_params)
    q = 3


if __name__ == '__main__':
    individual_offer_scan('https://www.otodom.pl/pl/oferta/2-pokojowe-mieszkanie-52m2-2-balkony-ID4onng')
