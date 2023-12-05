import requests
import re
import logging
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_links_from_url(url):
    logger.info(f'Extract links from {url} has started')
    link_list = []

    response = requests.get(url)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')

    teaser_wrappers = soup.find_all('div', class_='listing__teaserWrapper')

    for teaser_wrapper in teaser_wrappers:
        link = teaser_wrapper.find('a', class_='teaserLink')

        if link:
            link_list.append(link.get('href'))

    logger.info(f'Extract links from {url} has finished')

    return link_list


def extract_page_number(url):
    response = requests.get(url)
    html = response.text

    soup = BeautifulSoup(html, 'html.parser')

    # Find the div with class "pagination container"
    pagination_div = soup.find('div', class_='pagination container')

    # Find the span with class "pagination__separator" within the pagination_div
    separator_span = pagination_div.find('span', class_='pagination__separator')

    # Find the following anchor tag
    page_number_anchor = separator_span.find_next('a')

    # Extract the page number from the anchor text
    page_number = page_number_anchor.text.strip()

    return page_number


def extract_parameters(url):
    # Fetch HTML content from the URL
    logger.info(f'Extract parameters from {url} has started')
    response = requests.get(url)
    html = response.text

    # Parse the HTML content
    soup = BeautifulSoup(html, 'html.parser')

    # Find the div with class "parameters__container"
    parameters_div = soup.find('div', class_='parameters__container')

    # Find all li elements within the parameters_div
    li_elements = parameters_div.find_all('li')

    # Extract information from each li element
    parameter_list = []
    for li in li_elements:
        # Find the span and b elements within the li
        span = li.find('span')
        b = li.find('b', class_='parameters__value')

        # Extract text content from span and b elements
        span_text = span.text.strip() if span else None
        b_text = b.text.strip() if b else None

        # Append the tuple to the parameter list
        parameter_list.append((span_text, b_text))

    # Extract price information
    price_info_div = soup.find('div', class_='priceInfo')
    price_span = price_info_div.find('span', class_='priceInfo__value')
    additional_price_span = price_info_div.find('span', class_='priceInfo__additional')

    # Extract text content from price and additional price spans
    price_text = price_span.text.strip() if price_span else None
    additional_price_text = additional_price_span.text.strip() if additional_price_span else None

    # Append the price information to the parameter list
    parameter_list.append(('price', price_text))
    parameter_list.append(('additional_price', additional_price_text))

    # Append info about photos
    image_list = extract_images_from_script(html)
    parameter_list.append(('image_list', image_list))
    logger.info(f'Extract parameters from {url} has finished')
    return parameter_list


def extract_images_from_script(html_parsed):
    try:
        logger.info(f'Extract images has started')
        soup = BeautifulSoup(html_parsed, 'html.parser')

        # Find the script element within offer__relativeBox
        script_element = soup.find('div', class_='offer__relativeBox')
        image_urls = []

        if script_element:
            script_element = script_element.find('script')
            # Extract the content of the script tag
            script_content = script_element.string.replace('\\/', '/')

            # Use regular expression to find all image URLs with [xlarge] in the name
            image_urls = re.findall(r'"url":"(https://[^"]+_xlarge\.jpg)"', script_content)

    except Exception as e:
        # Handle exceptions here, you can print an error message or log the exception
        print(f"An error occurred: {e}")
        image_urls = []  # You might want to set image_urls to an empty list or handle it based on your requirements
    logger.info(f'Extract images has finished')
    return image_urls


def prepare_links(url, number):
    links = []
    logger.info(f'Preparing links for {url} has started')
    for i in range(1, number + 1):
        links.append(f'{url}?page={i}')
    logger.info(f'Preparing links for {url} has finished')
    return links
