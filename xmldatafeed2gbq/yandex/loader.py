import requests
import json

ZONE_1 = [  'Адлер', 'Альметьевск', 'Арзамас', 'Астрахань', 'Батайск', 'Белгород', 'Бердск', 'Брянск', 'Великий Новгород', 'Владимир', 
            'Волгоград', 'Волжский', 'Воронеж', 'Дзержинск', 'Димитровград', 'Екатеринбург', 'Елец', 'Железногорск', 'Иваново', 'Ижевск', 
            'Йошкар-Ола', 'Казань', 'Калуга', 'Каменск-Уральский', 'Киров', 'Ковров', 'Колпино', 'Копейск', 'Кострома', 'Краснодар', 
            'Красноярск', 'Курск', 'Липецк', 'Миасс', 'Москва', 'Московская область', 'Набережные Челны', 'Нижнекамск', 'Нижний Новгород', 
            'Новокуйбышевск', 'Новомосковск', 'Новороссийск', 'Новосибирск', 'Новочебоксарск', 'Новочеркасск', 'Новошахтинск', 'Обнинск', 
            'Омск', 'Орёл', 'Оренбург', 'Пенза', 'Первоуральск', 'Пермь', 'Пушкин', 'Ростов-на-Дону', 'Рыбинск', 'Рязань', 'Самара', 
            'Санкт-Петербург', 'Саратов', 'Смоленск', 'Сочи', 'Сызрань', 'Таганрог', 'Тамбов', 'Тверь', 'Тольятти', 'Тула', 'Тюмень', 
            'Ульяновск', 'Уфа', 'Чебоксары', 'Челябинск', 'Шахты', 'Энгельс', 'Ярославль']

ZONE_2 = [  'Абакан', 'Ангарск', 'Армавир', 'Архангельск', 'Ачинск', 'Балаково', 'Барнаул', 'Березники', 'Бийск', 'Владикавказ', 'Волгодонск', 
            'Вологда', 'Грозный', 'Дербент', 'Евпатория', 'Ессентуки', 'Златоуст', 'Иркутск', 'Калининград', 'Камышин', 'Каспийск', 'Кемерово', 
            'Керчь', 'Кисловодск', 'Курган', 'Кызыл', 'Магнитогорск', 'Майкоп', 'Махачкала', 'Мурманск', 'Муром', 'Назрань', 'Нальчик', 
            'Невинномысск', 'Нефтекамск', 'Нефтеюганск', 'Нижневартовск', 'Нижний Тагил', 'Новокузнецк', 'Октябрьский', 'Орск', 'Петрозаводск', 
            'Прокопьевск', 'Псков', 'Пятигорск', 'Рубцовск', 'Салават', 'Саранск', 'Севастополь', 'Северодвинск', 'Северск', 'Симферополь', 
            'Ставрополь', 'Старый Оскол', 'Стерлитамак', 'Сургут', 'Сыктывкар', 'Тобольск', 'Томск', 'Улан-Удэ', 'Череповец', 'Черкесск', 'Элиста']

ZONE_3 = [  'Артём', 'Благовещенск', 'Братск', 'Владивосток', 'Комсомольск-на-Амуре', 'Находка', 'Новый Уренгой', 'Норильск', 'Ноябрьск', 
            'Петропавловск-Камчатский', 'Уссурийск', 'Хабаровск', 'Хасавюрт', 'Чита', 'Южно-Сахалинск', 'Якутск']


BOXBERRY_SERVICE_ID = 106
DELIVER_CDEK_ID = 51


def get_all_points_yandex(campaign_id_yandex, oauth_token_yandex, oauth_id_yandex):
    """
    The function returns the names of all existing delivery points from the current campaign in Yandex

    @param campaign_id_yandex   - Yandex campaign ID
    @param oauth_token_yandex   - token for access to Yandex API
    @param oauth_id_yandex      - application ID in Yandex

    @return set of data in Yandex format about already uploaded points
    """

    url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id_yandex}/outlets.json'

    headers = {
        'Authorization': f'OAuth oauth_token={oauth_token_yandex}, oauth_client_id={oauth_id_yandex}',
        'Content-Type' : 'application/json'
    }
    data = requests.request("GET", url, headers=headers).json()
    
    result = set()

    print('Начинается загрузка уже загруженных ПВЗ...')
    while True:
        try:
            for item in data['outlets']:
                result.add(item['name'])
            print(f'Загружены данные о {len(result)} точках')
            print('-'*100)

            next_page_token = data['paging']['nextPageToken']

            url = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id_yandex}/outlets.json?page_token={next_page_token}'

            data = requests.request("GET", url, headers=headers).json()


        except KeyError:
            return result


def get_city_code_yandex(name, area, oauth_token_yandex, oauth_id_yandex):
    """
    The function returns the city ID by its name from the Yandex API

    @param name                 - city name
    @param area                 - the area in which the city is located
    @param oauth_token_yandex   - token for access to Yandex API
    @param oauth_id_yandex      - application ID in Yandex

    @return city ID in the Yandex system
    """

    url = f'https://api.partner.market.yandex.ru/v2/regions.json?name={name}'

    headers = {
        'Authorization': f'OAuth oauth_token={oauth_token_yandex}, oauth_client_id={oauth_id_yandex}',
        'Content-Type' : 'application/json'
    }
    data = requests.request("GET", url, headers=headers).json()
    
    code = -1

    for item in data['regions']:        
        code = __find_necessary_code(item, area, 0)
        if code != -1: break

    return code


def get_yandex_type(service_name, data, city_code, already_loaded_points, deliver_rule = {}):
    """
    The function converts data from sender format to Yandex format
    It also checks if the current delivery point already exists in the Yandex campaign

    @service_name                   -   name of sender company
    @param data                     -   data about points
    @param city_code                -   Yandex city ID
    @param already_loaded_points    -   set of points already loaded
    @param deliver_rule             -   terms of delivery of the product from the sender
                                        Mandatory parameter for CDEK

    @return returns the list of these points in Yandex format
    """

    if len(data) == 0: return []

    if(service_name.lower() == 'boxberry'): return __get_yandex_type_by_boxberry(data, city_code, already_loaded_points)
    elif(service_name.lower() == 'cdek'):   return __get_yandex_type_by_cdek(data, city_code, already_loaded_points, deliver_rule)
    

def upload_points_yandex(data, campaign_id_yandex, oauth_token_yandex, oauth_id_yandex):
    """
    The function uploads data about delivery points to Yandex

    @param data                 - data about delivery points
    @param campaign_id_yandex   - campaign ID from Yandex
    @param oauth_token_yandex   - token for access to Yandex API
    @param oauth_id_yandex      - application ID in Yandex
    """

    url_dbs = f'https://api.partner.market.yandex.ru/v2/campaigns/{campaign_id_yandex}/outlets.json'

    headers = {
        'Authorization': f'OAuth oauth_token={oauth_token_yandex}, oauth_client_id={oauth_id_yandex}',
        'Content-Type' : 'application/json'
    }

    for i in range(len(data)):
        response = requests.request("POST", url_dbs, headers=headers, data=json.dumps(data[i]))
        print(f"Загружена {i+1} точка под названием: [{data[i]['name']}]")

        try:
            if response.json()['status'] != 'OK': print('Error in request under this msg...')
        except UnboundLocalError:
            print('Все ПВЗ уже загружены.')


def __find_necessary_code(json, area, deep):
    """
    The function finds the ID of the desired city by the name of the region among cities with the same name

    @param json - part of json file
    @param area - the area in which the city is located
    @param deep - the file level at which the script is now
    
    @return node id from the json file which contains the required city id
    """
 
    if json['type'] == 'REPUBLIC' and (json['name'].split(' ')[0] == area or 
        json['name'].split(' ')[0] == 'Республика' and json['name'].split(' ')[1] == area): return 1 
    else:
        try:
            retrieved = __find_necessary_code(json['parent'], area, deep + 1)
            if retrieved == 1:
                if deep == 0: return json['id']
                else: return 1
            else: return -1
        except KeyError: return -1


def __get_format_phone(phone_number, service):
    """
    The function converts the phone number from the sender standard to the Yandex standard

    @param phone_number - phone number in Boxberry standart
    @param service      - sender's service name

    @return phone number in Yandex format
    """

    if(service.lower() == 'cdek'): 

        return f'{phone_number[:2]} ({phone_number[2:5]}) {phone_number[5:8]}-{phone_number[8:10]}-{phone_number[10:]}'

    elif(service.lower() == 'boxberry'):

        try:
            if phone_number[1] == '-':
                return f'+{phone_number[0]} ({phone_number[2:5]}) {phone_number[6:]}'
            return f'{phone_number[:2]} {phone_number[2:7]} {phone_number[7:]}'
        except IndexError:
            return -1

    return -1


def __get_shedule_items(source_shedule, service):
    """
    The function converts the delivery point schedule from sender format to Yandex format

    @param source_shedule   - The line that contains information about the schedule
    @param service          - sender's service name

    @return delivery shedule in Yandex format
    """
   
    result = []

    if(service.lower() == "cdek"):
        
        result.append(
            {
                'startDay'  : __get_format_day(1, 'cdek'),
                'endDay'    : __get_format_day(1, 'cdek'),
                'startTime' : source_shedule[0]['periods'].split('/')[0],
                'endTime'   : source_shedule[0]['periods'].split('/')[1]
            }
        )

        for item in source_shedule:
            if result[-1]['startTime'] == item['periods'].split('/')[0] and result[-1]['endTime'] == item['periods'].split('/')[1]:
                continue

            result[-1]['endDay'] = __get_format_day(int(item['day']) - 1, 'cdek')
            result.append(
                {
                    'startDay'  : __get_format_day(int(item['day']), 'cdek'),
                    'endDay'    : __get_format_day(int(item['day']), 'cdek'),
                    'startTime' : item['periods'].split('/')[0],
                    'endTime'   : item['periods'].split('/')[1]
                }
            )

        result[-1]['endDay'] = __get_format_day(7, 'cdek')

    elif(service.lower() == "boxberry"):

        if(len(source_shedule) == 0): return -1

        while source_shedule.find(',') != -1:
            first_ind = source_shedule.find(',')
            source_shedule = source_shedule[:first_ind] + source_shedule[first_ind + 19:]

        shedule = []
        for item in (source_shedule[0 + i : 16 + i] for i in range(0, len(source_shedule), 16)):
            if __get_format_day(item[:2], 'boxberry') == -1: return -1

            buff = item.split(' ')
            shedule.append([buff[0], buff[1]])

        for item in shedule:
            same_time_shedule = False
            for item_result in result:
                if item_result['startTime'] == item[1].split('-')[0] and item_result['endTime'] == item[1].split('-')[1]:
                    item_result['endDay'] = __get_format_day(item[0][:-1], 'boxberry')
                    same_time_shedule = True

            if same_time_shedule: continue

            result.append({
                'startDay'  : __get_format_day(item[0][:-1], 'boxberry'),
                'endDay'    : __get_format_day(item[0][:-1], 'boxberry'),
                'startTime' : item[1].split('-')[0],
                'endTime'   : item[1].split('-')[1]
            })

    return result


def __get_format_day(day_of_week, service):
    """
    The function converts the day of the week from the sender standard to the Yandex standard

    @param day_of_week  - value of current day
    @param service      - sender's service name

    @return day in Yandex format
    """

    if(service.lower() == 'cdek'):

        if day_of_week == 1:
            return 'MONDAY'
        elif day_of_week == 2:
            return 'TUESDAY'
        elif day_of_week == 3:
            return 'WEDNESDAY'
        elif day_of_week == 4:
            return 'THURSDAY'
        elif day_of_week == 5:
            return 'FRIDAY'
        elif day_of_week == 6:
            return 'SATURDAY'
        elif day_of_week == 7:
            return 'SUNDAY'

    elif(service.lower() == 'boxberry'):

        if day_of_week == 'пн':
            return 'MONDAY'
        elif day_of_week == 'вт':
            return 'TUESDAY'
        elif day_of_week == 'ср':
            return 'WEDNESDAY'
        elif day_of_week == 'чт':
            return 'THURSDAY'
        elif day_of_week == 'пт':
            return 'FRIDAY'
        elif day_of_week == 'сб':
            return 'SATURDAY'
        elif day_of_week == 'вс':
            return 'SUNDAY'

    return -1


def __get_yandex_type_by_cdek(data, city_code, already_loaded_points, deliver_rule):
    """
    The function converts point data from CDEK format to Yandex format

    @param data                     - data about points
    @param city_code                - Yandex city ID
    @param already_loaded_points    - set of points already loaded
    @param deliver_rule             - terms of delivery of the product from the sender

    @return returns the list of these points in Yandex format
    """

    if deliver_rule == {}: return []

    all_items = []
    for item in data:
        if f"{item['name']} [{item['code']}]" in already_loaded_points: 
            print(f"ПВЗ с именем {item['name']} уже загружена.")
            continue

        sheduleItems = __get_shedule_items(item['workTimeYList'], 'cdek')

        if sheduleItems == -1: continue    

        phone_number = __get_format_phone(item['phoneDetailList'][0]['number'], 'cdek')

        descr = item['fullAddress']
        if len(descr) > 250: descr = descr[:251]

        all_items.append({
            'name'              : f"{item['name']} [{item['code']}]",
            'type'              : 'DEPOT',
            'coords'            : ', '.join([item['coordX'], item['coordY']]),
            'address'           : {
                'regionId'          : city_code,
                'street'            : item['address'].split(',')[0],
                'number'            : item['address'].split(',')[1],
                'additional'        : descr
            },
            'phones'            : [phone_number],
            'workingSchedule'   : {
                'scheduleItems'     : sheduleItems,
            },
            'deliveryRules'     : deliver_rule,
        })

        return all_items


def __get_yandex_type_by_boxberry(data, city_code, already_loaded_points):
    """
    The function converts point data from Boxberry format to Yandex format

    @param data                     - data about points
    @param city_code                - Yandex city ID
    @param already_loaded_points    - set of points already loaded
    
    @return returns the list of these points in Yandex format
    """

    print('Преобразование элементов к формату Яндекса...')

    all_items = []
    for item in data:
        try:
            if item['Name'] in already_loaded_points: 
                print(f"ПВЗ с именем {item['Name']} уже загружена.")
                continue
        except KeyError:
            print('Данные о данном городе отсутствуют в Boxberry')
            return []

        sheduleItems = __get_shedule_items(item['WorkShedule'], 'boxberry')

        if len(item['AddressReduce']) == 0: continue
        elif sheduleItems == -1: continue

        coords = item['GPS'].split(',')

        cost = 0

        if item['CityName'] in ZONE_1 or item['CityName'] in ZONE_2: cost = 49
        elif item['CityName'] in ZONE_3: cost = 799


        rule = []
        rule.append({
            'cost'              : cost,
            'minDeliveryDays'   : int(item['DeliveryPeriod']),
            'maxDeliveryDays'   : int(item['DeliveryPeriod']) + 1,
            'deliveryServiceId' : BOXBERRY_SERVICE_ID
        })

        phone_number = __get_format_phone(item['Phone'], 'boxberry')
        if phone_number == -1: continue

        descr = item['TripDescription']
        if len(descr) > 250: descr = descr[:251]

        all_items.append({
            'name'              : item['Name'],
            'type'              : 'DEPOT',
            'coords'            : ', '.join([coords[1], coords[0]]),
            'address'           : {
                'regionId'          : city_code,
                'street'            : item['AddressReduce'].split(',')[0],
                'number'            : item['AddressReduce'].split(',')[1],
                'additional'        : descr
            },
            'phones'            : [phone_number],
            'workingSchedule'   : {
                'scheduleItems'     : sheduleItems,
            },
            'deliveryRules'     : rule,
        })

    return all_items