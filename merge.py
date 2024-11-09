import pandas as pd
import os, json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from bs4 import BeautifulSoup
import requests as req
import logging, time, datetime, re
import json

logging.basicConfig(level=logging.DEBUG, filename='logs.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf8', datefmt='%Y-%m-%d %H:%M:%S')


directory = r'H:\karma\Desktop\parserTest\results'

def set_info(message: str):
    print(datetime.datetime.now(), message)
    logging.debug(message)




def parse_href(href: str):
    pattern = r'([-\d.]+),([-\d.]+)'
    numbers_match = re.search(pattern=pattern, string=href)
    return numbers_match.group(0)

def main():
    dataframe = pd.DataFrame()
    set_info('Чтение файлов')
    for file in tqdm(os.listdir(directory), total=len(os.listdir(directory))):
        filepath = os.path.join(directory, file)
        with open(os.path.join(directory, file), 'r') as f:
            df = pd.read_excel(filepath)
            dataframe = pd.concat([dataframe, df], ignore_index=True)
    

    hrefs = dataframe[dataframe['city'].isna()]['href'].tolist()
    ids = dataframe[dataframe['city'].isna()]['id'].tolist()
    set_info('Исправление таблицы')
    with Session() as s:
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(tqdm(executor.map(lambda x: parse_page(x[0], x[1], s),  zip(ids, hrefs)), total=len(hrefs)))

    set_info('Результаты получены. Теперь объедение и исправление основной таблицы')
    for href, result in zip(hrefs, results):
        data = pd.DataFrame(result)
        index = dataframe[dataframe['href'] == href].index
        print(index)
        data.index = index
        dataframe.update(pd.DataFrame(data), overwrite=True)

    dataframe = dataframe.drop(columns=dataframe.columns[dataframe.columns.str.contains('Unnamed')])
    print(dataframe.head(5))
    with open('result.csv', 'wb') as file:
        dataframe.to_csv(file, index=False)
    dataframe.to_excel('result.xlsx', index=False)
    set_info('Исправление и запись в файл закончены')

from random import uniform
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 YaBrowser/24.10.0.0 Safari/537.36",
        'Accept': "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "ru,en;q=0.9"
}
from requests import Session, exceptions


def parse_page(id: str, page: str, s: Session):
    s.headers.update(headers)
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = s.get(page)
            if response.status_code == 200:
                break
        except exceptions.ConnectionError:
            if attempt < max_retries - 1:
                print("Ошибка соединения, повтор попытки...")
                time.sleep(uniform(1, 3))
            else:
                raise

    if response.status_code == 200:
        
        data = response.content.decode('utf-8', errors='replace')
        bs = BeautifulSoup(data, 'lxml')
        prop = {}
        prop['id'] = [id]
        prop['href'] = [page]
        prop_name = bs.select_one('h1').text
        if '404' in prop_name:
            set_info(f"{page}: не существует! Пропускается парсинг")
            return
        prop['property'] = [prop_name]
        
        aside = bs.find(class_='aside')
        if aside:
            params = aside.find(class_='params')
            if params:
                names = [k.text for k in params.find_all(class_='name')]
                values = [v.contents[0] for v in params.find_all(class_='value')]
                for name, value in zip(names, values):
                    prop[name] = [value]
            
        
            seller_name = aside.select_one('div.contact_agency.right_block > div.company.center > div.info > div > a')
            prop['seller_name'] = [seller_name.text.strip() if seller_name else '']
            seller_href = aside.select_one('div.contact_agency.right_block > div.company.center > div.info > div > a')
            prop['seller_href'] = [seller_href.get('href', '') if seller_href else '']
        
        description = bs.select_one('div[itemprop="description"]')
        prop['description'] = [description.text.strip() if description else '']


        features = bs.select('div.features')
        if features:
            for item in features:
                prop[item.select_one('h3').text] = [', '.join([i.text for i in item.select_one('ul').contents])]
        
        # {item.select_one('h3').text: ', '.join([i.text for i in item.select_one('ul').contents]) for item in bs.select('div.features')}
        map_block = bs.select_one('div#map_block')
        if map_block:
            prop['coordinates'] = [parse_href(map_block.select('ul li')[1]['onclick'])]
        return prop
    else:
        set_info(f'страница не загружается {page}')





if __name__ == '__main__':
    main()