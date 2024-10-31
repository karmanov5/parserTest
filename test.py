from selenium.webdriver.common.by import By
from selenium.webdriver import Edge
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
from time import sleep
import pandas as pd
import re
import logging
import pickle
from tqdm import tqdm
from os import path
from concurrent.futures import ThreadPoolExecutor
from main import parse_page


df = pd.DataFrame()

logging.basicConfig(level=logging.DEBUG, filename='logs.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', encoding='utf8')
logging.getLogger('selenium').setLevel(logging.WARNING)

options = Options()
# prefs = {"profile.managed_default_content_settings.images": 2}
# options.add_experimental_option("prefs", prefs)
options.use_chromium = True
options.add_argument('--headless')
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("disable-infobars")

# urls = ['https://thailand-real.estate/', 'https://thailand-real.estate/property/so-origin-bangtao-beach-82348/']

# def process_data(param1, param2):
#     # Здесь выполняется обработка данных
#     return f"Processed {param1} and {param2}"

# # Списки данных
# list1 = [1, 2, 3]
# list2 = ['a', 'b', 'c']

# # Создаем ThreadPoolExecutor для многопоточного выполнения
# with ThreadPoolExecutor(max_workers=5) as executor:
#     # Используем zip для объединения списков и executor.map для передачи данных в функцию
#     results = list(executor.map(lambda x: process_data(*x), zip(list1, list2)))

# Выводим результаты
# for result in results:
#     print(result)

import os
def main():

    for file in os.listdir('results/'):

        df = pd.read_excel(f'results/{file}')
        hrefs = df[(df['property'] == 'Не удается открыть эту страницу') | df['property'].isna()]['href'].tolist()
        ids = df[(df['property'] == 'Не удается открыть эту страницу') | df['property'].isna()]['id'].tolist()
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(tqdm(executor.map(lambda x: parse_page(*x),  zip(ids, hrefs)), total=len(hrefs)))

        for href, result in zip(hrefs, results):
            data = pd.DataFrame(result)
            data.index = df[df['href'] == href].index
            df.update(pd.DataFrame(data), overwrite=True)
            
        print(df[df['property'] == 'Не удается открыть эту страницу'])

        df.to_excel(f"results/{file.replace('.xlsx', '') + ' new'}.xlsx")
    


if __name__ == '__main__':
    main()