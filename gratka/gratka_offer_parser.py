from .gratka_db_schema import Offer


def offer_parse_parameters(params):
    table_columns = {'offer_id': None,
                     'website_address': None,
                     'city': None,
                     'district': None,
                     'price': None,
                     'area': None,
                     'price_per_square_meter': None,
                     'floor': None,
                     'number_of_rooms': None,
                     'building_type': None,
                     'ownership_type': None,
                     'year_of_construction': None,
                     'date_added': None,
                     'date_removed': None,
                     'last_visited': None
                     }

    expected_params = {'Lokalizacja': None,
                       'Typ zabudowy': None,
                       'Forma własności': None,
                       'Piętro': None,
                       'Rok budowy': None,
                       'Liczba pokoi': None,
                       'Powierzchnia w m2': None,
                       'additional_price': None,
                       'price': None}

    for param in params:
        if param[0] in expected_params:
            expected_params[param[0]] = param[1]

    location_params = expected_params['Lokalizacja'].split(',')

    table_columns['city'] = location_params[0].strip()
    table_columns['district'] = location_params[1].strip()
    table_columns['building_type'] = expected_params['Typ zabudowy']
    table_columns['ownership_type'] = expected_params['Forma własności']
    table_columns['floor'] = expected_params['Piętro']
    table_columns['year_of_construction'] = int(expected_params['Rok budowy']) if expected_params[
        'Rok budowy'] else None
    rooms = expected_params['Liczba pokoi'] if expected_params['Liczba pokoi'] else 0
    try:
        rooms = int(rooms)
    except ValueError:
        rooms = 0

    table_columns['number_of_rooms'] = rooms
    area = expected_params['Powierzchnia w m2'][:-3].strip().replace(' ', '').replace(',', '.') if expected_params[
        'Powierzchnia w m2'] else None
    table_columns['area'] = float(area) if area else None
    price_per_square_meter = expected_params['additional_price'][:-6].replace(' ', '').replace(',', '.') if \
        expected_params['additional_price'] else None
    table_columns['price_per_square_meter'] = float(price_per_square_meter) if price_per_square_meter else None
    price = expected_params['price'][:-2].strip().replace(' ', '').replace(',', '.') if expected_params[
        'price'] else None
    price = None if price == 'Zapytajoce' else price
    table_columns['price'] = float(price) if price else None

    return Offer(
        city=table_columns['city'],
        district=table_columns['district'],
        price=table_columns['price'],
        area=table_columns['area'],
        price_per_square_meter=table_columns['price_per_square_meter'],
        floor=table_columns['floor'],
        number_of_rooms=table_columns['number_of_rooms'],
        building_type=table_columns['building_type'],
        ownership_type=table_columns['ownership_type'],
        year_of_construction=table_columns['year_of_construction']
    )
