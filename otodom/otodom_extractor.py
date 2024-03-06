import logging
import requests
import json
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PREFIX_URL = "https://www.otodom.pl"


def extract_page_number(url):

    headers = {
        'User-Agent': 'PostmanRuntime/7.36.3',
        'Cookie': 'lang=pl'}

    response = requests.get(url, headers=headers)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')
    buttons = soup.select('ul[data-testid="frontend.search.base-pagination.nexus-pagination"] li')

    # Check if there are at least two buttons
    if buttons:
        second_to_last_button = buttons[-2]
        value = second_to_last_button.text.strip()
        return int(value)
    else:
        return 1


def extract_links_from_url(url):

    headers = {
        'User-Agent': 'PostmanRuntime/7.36.3',
        'Cookie': 'lang=pl'}

    response = requests.get(url, headers=headers)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')
    links = soup.select('a[data-cy="listing-item-link"]')

    href_values = [f"{PREFIX_URL}{link.get('href')}" for link in links]
    return href_values


def extract_parameters(url):
    headers = {
        'User-Agent': 'PostmanRuntime/7.36.3',
        'Cookie': 'lang=pl'}

    response = requests.get(url, headers=headers)
    html = response.text

    parameter_list = []

    if (len(response.history) > 0 and 'from404' in response.history[0].text) or is_offer_removed(html):
        parameter_list.append('Removed')
        return parameter_list

    # Parse the HTML content
    soup = BeautifulSoup(html, 'html.parser')

    extract_detailed_info(parameter_list, soup, 'ad.top-information.table')
    extract_detailed_info(parameter_list, soup, 'ad.additional-information.table')
    extract_price_district(parameter_list, soup)
    extract_image_address(parameter_list, html)

    return parameter_list


def extract_detailed_info(parameter_list, soup, page_element):
    # Locate the div with the specified data-testid
    target_div = soup.find('div', {'data-testid': page_element})
    # Extract all divs inside target_div
    if target_div:
        target_div = target_div.find_all('div', {'role': 'region'})
        excluded_tags = ['Obsługa zdalna']
        for child_div in target_div:
            # Extract label and value
            label = child_div.find('div', {'data-cy': 'table-label-content'}).text.strip()
            if label not in excluded_tags:
                # Check if there is a button inside the current div
                button = child_div.find('button', {'data-cy': 'missing-info-button'})
                if button:
                    value = button.text.strip()
                else:
                    value = child_div.find('div', {'data-testid': True}).text.strip()

                # Append the pair as a tuple to the list
                parameter_list.append((label, value))


def extract_price_district(parameter_list, soup):
    price = soup.find('strong', {'aria-label': 'Cena'}).text.strip()
    price_per_square_meter = soup.find('div', {'aria-label': 'Cena za metr kwadratowy'}).text.strip()
    address = soup.find('a', {'aria-label': 'Adres'}).text.strip()

    parameter_list.append(('price', price))
    parameter_list.append(('price_per_square_meter', price_per_square_meter))
    parameter_list.append(('address', address))


def extract_image_address(parameter_list, html):
    soup = BeautifulSoup(html, 'html.parser')
    script = soup.find('script', {'id': '__NEXT_DATA__'}).text
    data = json.loads(script)
    thumbnails_list = data['props']['pageProps']['ad']['images']
    large_image_urls = [image['large'] for image in thumbnails_list]
    parameter_list.append(('images', large_image_urls))


def is_offer_removed(html):
    soup = BeautifulSoup(html, 'html.parser')
    removed_message = soup.find('div', {'data-cy': 'expired-ad-alert'}) or soup.find('div', {
        'data-cy': 'redirectedFromInactiveAd'})
    return removed_message is not None


def prepare_links(url, number):
    links = []
    separator = '&' if '?' in url else '?'
    logger.info(f'Preparing links for {url} has started')
    for i in range(1, int(number) + 1):
        links.append(f'{url}{separator}page={i}')
    logger.info(f'Preparing links for {url} has finished')
    return links
