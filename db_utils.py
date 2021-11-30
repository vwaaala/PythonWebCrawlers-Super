extracted_data = {
    'id': '16629',
    'link': 'https://aukcje.ideagetin.pl/aukcja/16629/naczepa-podkontenerowa-cimc-sc03/',
    'category_id': 3,
    'category': 'przyczepy-naczepy',
    'title': 'Naczepa podkontenerowa CIMC SC03',
    'start': '',
    'stop': 'Do końca: 5 dni 1 godzina (2021-12-03 14:00:00)',
    'type': '-',
    'paramList': 'marka:cimc|rokprodukcji:2017|nr rejestracyjny:dsr3814p|wyposażenie:[]',
    'description': 'Pojazd posiada polisę postojową.',
    'price': 60300.0,
    'flag': 0,
    'images': 'https://aukcje.ideagetin.pl//i/zd/zdjecie-294667_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd'
              '/zdjecie-294668_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd/zdjecie-294669_898x595_resize.jpg'
              '|https://aukcje.ideagetin.pl//i/zd/zdjecie-294670_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd'
              '/zdjecie-294671_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd/zdjecie-294672_898x595_resize.jpg'
              ''}

# str = 'marka:cimc|rok produkcji:2017|nr rejestracyjny:dsr3814p|wyposażenie:[]'
# # split string and convert it to dictionary
# data = dict(x.split(":") for x in str.split("|"))
# # change key name
# new_data = {"brand" if k == "marka" else k: v for k, v in data.items()}
# # capitalize value of marka key
# marka = data.get('marka').capitalize()
# # update dictionary with modified value
# data['marka'] = marka
# print(data)
# Product_list ={}
# Product_list = {x.replace(' ', ''): v for x, v in Product_list.items()}
# paramList = 'marka:cimc|rok produkcji:2017|nr rejestracyjny:dsr3814p|wyposażenie:[]'
result = {
    'id': '16629',
    'link': 'https://aukcje.ideagetin.pl/aukcja/16629/naczepa-podkontenerowa-cimc-sc03/',
    'category_id': 3,
    'category': 'przyczepy-naczepy',
    'title': 'Naczepa podkontenerowa CIMC SC03',
    'start': '',
    'stop': 'Do końca: 5 dni 1 godzina (2021-12-03 14:00:00)',
    'type': '-',
    'paramList': {
        'brand': 'Cimc',
        'rokprodukcji': '2017',
        'registration_number': 'dsr3814p',
        'wyposażenie': '[]'
    },
    'description': 'Pojazd posiada polisę postojową.',
    'price': 60300.0,
    'flag': 0,
    'images': 'https://aukcje.ideagetin.pl//i/zd/zdjecie-294667_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd'
              '/zdjecie-294668_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd/zdjecie-294669_898x595_resize.jpg'
              '|https://aukcje.ideagetin.pl//i/zd/zdjecie-294670_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd'
              '/zdjecie-294671_898x595_resize.jpg|https://aukcje.ideagetin.pl//i/zd/zdjecie-294672_898x595_resize.jpg'}


def modify_dict(data: str) -> dict:
    data = dict(x.split(":") for x in data.split("|"))
    data = {k.replace(' ', '_'): v for k, v in data.items()}
    data = {"brand" if k == "marka" else k: v for k, v in data.items()}
    brand = data.get('brand').capitalize()
    data['brand'] = brand
    data = {"production_year" if k == "rok_produkcji" else k: v for k, v in data.items()}
    data = {"registration_number" if k == "nr_rejestracyjny" else k: v for k, v in data.items()}
    return data


extracted_data['paramList'] = modify_dict(extracted_data['paramList'])

# brand = data.get('brand')
print(extracted_data)
