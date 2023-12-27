from otodom.otodom_db_schema import OtodomOffer


def offer_parse_parameters(params):
    table_columns = {'offer_id': None,
                     'website_address': None,
                     'parking_space': None,
                     'heating': None,
                     'additional_information': None,
                     'building_material': None,
                     'type_of_construction': None,
                     'rent': None,
                     'price': None,
                     'area': None,
                     'finishing_standard': None,
                     'security': None,
                     'balcony_garden_terrace': None,
                     'windows': None,
                     'year_of_construction': None,
                     'market': None,
                     'number_of_rooms': None,
                     'elevator': None,
                     'ownership_form': None,
                     'equipment': None,
                     'utilities': None,
                     'advertiser_type': None,
                     'price_per_square_meter': None,
                     'floor': None,
                     'address': None,
                     'last_visited': None,
                     'date_removed_datetime': None,
                     'date_added': None,
                     }

    expected_params = {'Miejsce parkingowe': None,
                       'Ogrzewanie': None,
                       'Informacje dodatkowe': None,
                       'Materiał budynku': None,
                       'Rodzaj zabudowy': None,
                       'Czynsz': None,
                       'price': None,
                       'Powierzchnia': None,
                       'Zabezpieczenia': None,
                       'Stan wykończenia': None,
                       'Balkon / ogród / taras': None,
                       'Okna': None,
                       'Rok budowy': None,
                       'Rynek': None,
                       'Liczba pokoi': None,
                       'Winda': None,
                       'Forma własności': None,
                       'Wyposażenie': None,
                       'Media': None,
                       'Typ ogłoszeniodawcy': None,
                       'price_per_square_meter': None,
                       'Piętro': None,
                       'address': None
                       }

    for param in params:
        if param[0] in expected_params:
            expected_params[param[0]] = param[1]

    table_columns['parking_space'] = expected_params['Miejsce parkingowe'] if expected_params['Miejsce parkingowe'] else None
    table_columns['heating'] = expected_params['Ogrzewanie'] if expected_params['Ogrzewanie'] else None
    table_columns['additional_information'] = expected_params['Informacje dodatkowe'] if expected_params['Informacje dodatkowe'] else None
    table_columns['building_material'] = expected_params['Materiał budynku'] if expected_params['Materiał budynku'] else None
    table_columns['type_of_construction'] = expected_params['Rodzaj zabudowy'] if expected_params['Rodzaj zabudowy'] else None
    table_columns['rent'] = expected_params['Czynsz'] if expected_params['Czynsz'] else None
    table_columns['price'] = (0 if 'Zapytaj' in expected_params['price'] else float(expected_params['price'][:-2].strip().replace(' ', '').replace(',', '.'))) if expected_params['price'] else None
    table_columns['area'] = float(expected_params['Powierzchnia'][:-3].strip().replace(' ', '').replace(',', '.')) if expected_params[
        'Powierzchnia'] else None
    table_columns['finishing_standard'] = expected_params['Stan wykończenia'] if expected_params['Stan wykończenia'] else None
    table_columns['security'] = expected_params['Zabezpieczenia'] if expected_params['Zabezpieczenia'] else None
    table_columns['balcony_garden_terrace'] = expected_params['Balkon / ogród / taras'] if expected_params['Balkon / ogród / taras'] else None
    table_columns['windows'] = expected_params['Okna'] if expected_params['Okna'] else None
    table_columns['year_of_construction'] = int(expected_params['Rok budowy']) if expected_params['Rok budowy'] else None
    table_columns['market'] = expected_params['Rynek'] if expected_params['Rynek'] else None
    rooms = expected_params['Liczba pokoi'] if expected_params['Liczba pokoi'] else 0
    try:
        rooms = int(rooms)
    except ValueError:
        rooms = 0
    table_columns['number_of_rooms'] = rooms
    table_columns['elevator'] = expected_params['Winda'] if expected_params['Winda'] else None
    table_columns['ownership_form'] = expected_params['Forma własności'] if expected_params['Forma własności'] else None
    table_columns['equipment'] = expected_params['Wyposażenie'] if expected_params['Wyposażenie'] else None
    table_columns['utilities'] = expected_params['Media'] if expected_params['Media'] else None
    table_columns['advertiser_type'] = expected_params['Typ ogłoszeniodawcy'] if expected_params['Typ ogłoszeniodawcy'] else None
    table_columns['price_per_square_meter'] = float(expected_params['price_per_square_meter'][:-6].replace(' ', '').replace(',', '.')) if expected_params['price_per_square_meter'] else None
    table_columns['address'] = expected_params['address'] if expected_params['address'] else None
    table_columns['floor'] = expected_params['Piętro'] if expected_params['Piętro'] else None

    return OtodomOffer(
        parking_space=table_columns['parking_space'],
        heating=table_columns['heating'],
        additional_information=table_columns['additional_information'],
        building_material=table_columns['building_material'],
        type_of_construction=table_columns['type_of_construction'],
        rent=table_columns['rent'],
        price=table_columns['price'],
        area=table_columns['area'],
        finishing_standard=table_columns['finishing_standard'],
        security=table_columns['security'],
        balcony_garden_terrace=table_columns['balcony_garden_terrace'],
        windows=table_columns['windows'],
        year_of_construction=table_columns['year_of_construction'],
        market=table_columns['market'],
        number_of_rooms=table_columns['number_of_rooms'],
        elevator=table_columns['elevator'],
        ownership_form=table_columns['ownership_form'],
        equipment=table_columns['equipment'],
        utilities=table_columns['utilities'],
        advertiser_type=table_columns['advertiser_type'],
        price_per_square_meter=table_columns['price_per_square_meter'],
        address=table_columns['address'],
        floor=table_columns['floor']
    )
