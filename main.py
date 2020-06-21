import requests
from bs4 import BeautifulSoup
import pymysql.cursors
from datetime import datetime
from multiprocessing import Pool, current_process
import numpy as np
import random
import time
from fake_useragent import UserAgent
import params
import proxyscrape

HOST = 'https://www.rusprofile.ru/'

URL1 = 'https://www.rusprofile.ru/codes/89220'
URL2 = 'https://www.rusprofile.ru/codes/429110'

collector = proxyscrape.create_collector('my-collector', 'https')


# получить содержимое страницы
def get_html(url, my_proxy=None):
    process_name = current_process().name
    connection_is_ok = False

    if my_proxy is None:
        proxy = collector.get_proxy()
    else:
        proxy = my_proxy

    try:
        proxy_str = proxy.host+":"+proxy.port
    except AttributeError:
        collector.clear_blacklist()
        proxy_str = ''

    if proxy_str == '':
        print("{}:Подключаюсь заново без прокси..".format(process_name))
    else:
        print('{}:Используется прокси {}..'.format(process_name, proxy_str))

    while(connection_is_ok is False):
        try:
            time.sleep(random.randrange(5,10))
            curtain_proxy = {
                'https': proxy_str
            }
            r = requests.get(url, headers={'user-agent': UserAgent().chrome,'accept': '*/*'},
                             proxies=curtain_proxy, timeout=25)
            connection_is_ok = True
            return r
        except requests.exceptions.ProxyError:
            print("{}:Ошибка подключения!".format(process_name))
            print("{}:Подключаюсь заново без прокси..".format(process_name))
            collector.blacklist_proxy(host=proxy.host,port=proxy.port)
            proxy_str = ''
        except requests.exceptions.ConnectTimeout:
            print("Долго подключается!")
            proxy = collector.get_proxy({'code': 'ru'})
            try:
                proxy_str = proxy.host + ":" + proxy.port
                print('{}:Используется прокси {}..'.format(process_name, proxy_str))
            except AttributeError:
                proxy_str = ''
                print('Подключение без прокси..')
        except requests.exceptions.SSLError:
            print("Ошибка SSL")
            proxy_str = ''
            print('Подключение без прокси..')
        except requests.exceptions.ReadTimeout:
            print("Ошибка ReadTimeout")


# получить все url (пагинация)
def get_num_page_urls(base_url):
    html = get_html(base_url,'')
    soup = BeautifulSoup(html.text, 'html.parser')
    try:
        search_result = soup.find('div', class_='search-result-paging')
        # минус 2 li - это стрелки влево-вправо
        lis = search_result.find('ul', class_='paging-list').find_all('li')
        count_page = len(lis) - 2
        answer = []
        for i in range(count_page):
            answer.append(base_url + '/{}'.format(i + 1))
        return answer
    except AttributeError:
        return [base_url]


# получает все ссылки из html
def get_refs(html):
    soup = BeautifulSoup(html.text, 'html.parser')
    divs = soup.find_all('div',class_='company-item__title')
    refs = []
    for div in divs:
        ref = div.find('a').get('href')
        refs.append(ref)
    return refs


empty_parse_values = ['','','','ликвидирована','','0']


# считать данные на странице
# Название ОГРН ОКПО Статус организации Дата регистрации Уставный капитал
def get_organization_data(html):
    # парсинг html
    try:
        soup = BeautifulSoup(html.text, 'html.parser')
    except:
        return empty_parse_values
    # Название
    try:
        name = soup.find('div', class_='company-header__row').find('h1').get_text(strip=True)
    except AttributeError:
        name = ''
    # ОГРН
    try:
        ogrn = soup.find('span', id='clip_ogrn').get_text()
    except AttributeError:
        ogrn = ''
    # ОКПО
    try:
        okpo = soup.find('span', id='clip_okpo').get_text()
    except AttributeError:
        okpo = ''
    # статус
    try:
        status = soup.find('div', class_='company-status')
        # первый - написан выше
        second_class = status.attrs['class'][1]
        if (second_class == 'active-yes'):
            status = 'действующая'
        elif (second_class == 'reorganizated'):
            status = 'в процессе ликвидации'
        else:
            status = 'ликвидирована'
    except AttributeError:
        status = 'ликвидирована'
    # дата и капитал
    try:
        company_rows = soup.find('div', class_='company-requisites').find_all('div',class_='company-row')
        company_info_texts = company_rows[1].find_all('dd',class_='company-info__text')
        try:
            date = company_info_texts[0].get_text()
        except IndexError:
            date = ''
        try:
            capital = company_info_texts[1].find('span').get_text()
        except IndexError:
            capital = '0'
    except AttributeError:
        date = ''
        capital = '0'

    # привести в нормальный вид для сохранения в базу
    try:
        date = datetime.strptime(date, '%d.%m.%Y').date()
    except ValueError:
        date = ''
    capital = capital.replace('руб.', '').replace(' ', '')

    answer = [name, ogrn, okpo, status, date, capital]
    return answer


# получить все данные
def get_data():
    # получить все url
    urls1 = get_num_page_urls(URL1)
    urls2 = get_num_page_urls(URL2)
    all_urls = np.concatenate((urls1,urls2))
    print(all_urls)

    print('Страницы получены. Получаем ссылки..')
    # для каждой найти ссылки
    refs = []
    for url in all_urls:
        refs = np.concatenate((refs, get_refs(get_html(url,''))))

    print('Ссылки получены.')
    len_refs = len(refs)
    print(len_refs)

    print('Считывание данных:')
    random.shuffle(refs)

    pool = Pool(processes=5)
    answer = pool.map(process_func,refs)
    return answer


def process_func(ref):
    print(ref)
    org_data = get_organization_data(get_html(HOST+ref))

    # иногда возвращаются пустые значения или неправильные данные
    # это происходит из-за защиты от прокси
    # поэтому проверим ещё раз без прокси

    # org_data[5][-1]!='0' <- если неправильные данные, в капитале написаны нереальные значения
    # и у всех неправильных данных что я видел - капиталы не делятся на 10
    # если есть реальные данные с подобным капиталом - их просто ещё раз проверят

    if org_data[5][-1] != '0' or org_data == empty_parse_values:
        org_data = get_organization_data(get_html(HOST+ref,''))
    return org_data


# загрузить в базу данных
# data - массив массивов
def sql_load_data(data):
    connection = pymysql.connect(host=params.HOST,
                                 user=params.USER,
                                 password=params.PASSWORD,
                                 db=params.DB,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            truncate_query = 'TRUNCATE TABLE organization_data;'
            alter_query = 'ALTER TABLE organization_data AUTO_INCREMENT=1;'
            cursor.execute(truncate_query)
            cursor.execute(alter_query)

            sql_query = 'INSERT ' \
                        'INTO organization_data(name, OGRN, OKPO, status, register_date, auth_capital) ' \
                        'VALUES'
            for i in data:
                sql_query += "('{}','{}','{}','{}','{}','{}'),".format(i[0],i[1],i[2],i[3],i[4],i[5])
            sql_query = sql_query[:-1]
            sql_query = sql_query.replace("''","NULL")
            print(sql_query)
            cursor.execute(sql_query)
        connection.commit()
    finally:
        connection.close()
    return True


if __name__ == "__main__" :
    sql_load_data(get_data())


