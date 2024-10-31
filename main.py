from selenium.webdriver.common.by import By
from selenium.webdriver import Edge
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webelement import WebElement
import pandas as pd
import re, pickle, logging, os, time, datetime, json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

options = Options()
options.add_argument("--headless")
options.use_chromium = True
options.add_argument('--disable-blink-features=AutomationControlled')
options.add_argument("disable-infobars")
options.add_argument("--log-level=3")


logging.basicConfig(level=logging.DEBUG, filename='logs.log', filemode='w', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', encoding='utf8', datefmt='%Y-%m-%d %H:%M:%S')
logging.getLogger('selenium').setLevel(logging.WARNING)



cookies = None

def load_cookies(driver: Edge):
    global cookies
    if cookies is None:
        if os.path.exists('cookies.pkl'):
            with open('cookies.pkl', 'rb') as file:
                cookies = pickle.load(file)
        else:
            return
    for cookie in cookies:
        driver.add_cookie(cookie)
    driver.refresh()


def get_locations(driver: Edge):
    return driver.find_element(By.XPATH, "/html/body/div[1]/div[3]/div[2]/div[2]/div/div").find_elements(By.CLASS_NAME, "item")

def get_cities(location : WebElement):
    return location.find_element(By.CLASS_NAME, 'bottom').find_elements(By.CLASS_NAME, "row")


def parse_page(id: str, page: str):
    set_info(f"парсится {page}")
    driver = Edge(options=options)
    driver.set_page_load_timeout(300)
    try:
        driver.get(page)
    except Exception:
        set_info(f'{page} долго загружается! Остановка загрузки сайта')
        driver.execute_script("window.stop();")
    
    # load_cookies(driver)

    try:
        accept_button = driver.find_element(By.CSS_SELECTOR, "#cookie_warning > div.accept > a")
        accept_button.click()
    except Exception:
        pass

    prop = {}
    prop['id'] = [id]
    prop['href'] = [page]
    prop_name = ""
    try:
        prop_name = driver.find_element(By.TAG_NAME, 'h1').text
    except Exception:
        pass
    if '404' in prop_name:
        set_info(f"{page}: не существует! Пропускается парсинг")
        return
    prop['property'] = [prop_name]
    try:
        params = driver.find_elements(By.XPATH, "//*[@class='aside']/div[1]/div[1]/div")
        for p in params:
            try:
                prop[p.get_attribute('class')] = p.find_element(By.XPATH, "span[2]").text.replace('\n', ', ')
            except Exception:
                set_info(f"{page}: ошибка в парсинге аттрибута в params")
                pass
    except Exception:
        set_info(f'{page}: Нет элемента params property!')
        pass
    prop_seller_name, prop_seller_href = "", ""
    try:
        prop_seller = driver.find_element(By.XPATH, "//*[@class='aside']/div[2]/div[3]/div[1]//a")
        prop_seller_name, prop_seller_href = prop_seller.text, prop_seller.get_attribute('href')
    except Exception:
        set_info(f'{page}: ошибка в парсинге seller')
        pass
    prop['seller_name'] = [prop_seller_name]
    prop['seller_href'] = [prop_seller_href]
    prop_description = ""
    try:
        prop_description_path = driver.find_element(By.XPATH, "//*[@itemprop='description']")
        try:
            show_hidden = driver.find_element(By.XPATH, "//*[@itemprop='description']//*[contains(text(), 'Show full text')]")
            show_hidden.click()
        except Exception:
            pass
    
        for p in prop_description_path.find_elements(By.TAG_NAME, 'p'):
            prop_description += p.text + '\n'
    except Exception:
        set_info(f'{page}: отсутствует description')
        pass
    
    prop['description'] = [prop_description]

    try:
        features = driver.find_elements(By.XPATH, "//div[contains(@class, 'features')]")
        for f in features:
            try:
                more_button = f.find_element(By.TAG_NAME, 'a')
                more_button.click()
            except Exception:
                pass
            prop[f.find_element(By.TAG_NAME, 'h3').text.lower()] =[", ".join([e.text for e in f.find_elements(By.TAG_NAME, 'li')])]
    except Exception:
        set_info(f'{page}: отсутствует features')
        pass
    prop_coordinates = ""
    try:
        map_block = driver.find_element(By.XPATH, '//*[@id="map_block"]')
        driver.execute_script("arguments[0].scrollIntoView();", map_block)
        time.sleep(2.5)
        prop_coordinates = parse_href(driver.find_element(By.XPATH, '//*[@id="map"]/div/div[3]/div[13]/div/a').get_attribute('href'))
    # driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.END)
    except Exception:
        set_info(f'{page}: отсутствует map, невозможно запарсить координаты')
        pass
    
    prop['coordinates'] = [prop_coordinates]

    return prop


def parse_pages_for_list(page: str):
    set_info(f'парсится {page}')
    driver = Edge(options=options)
    driver.set_page_load_timeout(300)
    properity_ids = []
    properity_links = []
    try:
        driver.get(page)
    except Exception:
        set_info(f'{page} долго загружается! Остановка загрузки сайта')
        driver.execute_script("window.stop();")

    try:
        totals = driver.find_element(By.XPATH, "//*[@id='objects']/div[contains(@class, 'totals')]").text
        set_info(f'всего {totals} объектов недвижимости!')
    except Exception:
        set_info(f'на данной странице {page} 0 объектов недвижимости! Парсинг пропускается')
        return properity_ids, properity_links
        
    
    try:
        properties = [prop for prop in driver.find_element(By.XPATH, "//*[@id='objects']//div[contains(@class, 'objects-list') and contains(@class, 'listview')][1]/ul").find_elements(By.TAG_NAME, 'li') if prop.get_attribute('data-object') != None]
        properity_ids = [int(prop.get_attribute('data-object')) for prop in properties]
        properity_links = [prop.find_element(By.CLASS_NAME, 'title').find_element(By.TAG_NAME, 'a').get_attribute('href') for prop in properties]
    except Exception:
        set_info("возникла ошибка при парсинге страницы")
        pass
    driver.quit()
    set_info(f'окончен парсинг {page}')
    return properity_ids, properity_links


def parse_pages_for_links(city_name: str, city_url: str):
    set_info(f'парсинг города {city_name}: {city_url}')
    properity_ids = []
    properity_links = []
    # _options = Options()
    # _options.add_argument("--log-level=3")
    driver = Edge(options=options)
    driver.set_page_load_timeout(300)
    try:
        driver.get(city_url)
    except Exception:
        set_info(f'{city_url} долго грузится! Остановка загрузки сайта')
        driver.execute_script("window.stop();")
    
    
    title =  driver.find_element(By.TAG_NAME, 'h1').text
    if '404' in title:
        set_info(f'страницы {city_url} не существует! Пропускается парсинг!')
        return properity_ids, properity_links
    
    try:
        page_count = driver.find_element(By.XPATH, "//*[@id='objects']//ul[@class='pagination']").find_elements(By.TAG_NAME, "li")[-2].text
        set_info(f"{city_url}: страниц для парсинга {page_count} штук")
    except Exception:
        set_info(f"страница {city_url} с ссылками на объекты недвижимости одна!")
        page_count = 1
    
    driver.quit()
    

    page_urls_for_links = [city_url + f'page/{num}/#objects' for num in range(1, int(page_count) + 1)]
    set_info(f'парсинг ссылок на страницы города {city_name}')
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(tqdm(executor.map(parse_pages_for_list, page_urls_for_links), total=len(page_urls_for_links)))
    
    
    set_info(f'парсинг ссылок на страницы города {city_name} окончен')
    for worker in results:
        ids, links = worker
        properity_ids += ids
        properity_links += links

    set_info(f'сохранение ссылок на город {city_name}')
    file_dictionary = {k: v for k, v in zip(properity_ids, properity_links)}
    with open(f'cities/links_{city_name}.json', 'w', encoding='utf-8') as file:
        json.dump(file_dictionary, file, ensure_ascii=True, indent=4)
    set_info(f'сохранение ссылок на город {city_name} закончено') 
    return properity_ids, properity_links


def set_info(message: str):
    print(datetime.datetime.now(), message)
    logging.debug(message)


def main():
    set_info('начало парсинга')
    
    
    city_names = []
    city_urls = []
    loc_cities = {}
    cities_dict = {}
    set_info('проверка на файл cities.json')
    if os.path.exists('cities/cities.json'):
        set_info("чтение из файла")
        with open('cities/cities.json', 'r', encoding='utf-8') as file:
            loc_cities = json.load(file)
            city_names = list(loc_cities.keys())
            city_urls = list(loc_cities.values())
            cities_dict = {k: v for k, v in zip(city_names, city_urls)}
    else:
        driver = Edge(options=options)
        driver.get("https://thailand-real.estate/")
        try:
            accept_button = driver.find_element(By.CSS_SELECTOR, "#cookie_warning > div.accept > a")
            accept_button.click()
        except Exception:
            pass
        set_info('получение списка локаций')
        locations = get_locations(driver)
        set_info('получение списка городов из каждой локации')
       
        for item in locations:
            location_name = item.find_element(By.CLASS_NAME, "top").text
            loc_cities[location_name] = get_cities(item)


        for loc, cities in loc_cities.items():
            for city in cities:
                city_names.append(city.find_element(By.CLASS_NAME, "name").text)
                city_urls.append(city.find_element(By.TAG_NAME, "a").get_attribute("href"))
        driver.quit()

        set_info('Сохранение списка городов с их ссылками')
        cities_dict = {k: v for k, v in zip(city_names, city_urls)}
        with open('cities/cities.json', 'w', encoding='utf-8') as file:
            json.dump(cities_dict, file, ensure_ascii=True, indent=4)
        set_info('Список городов сохранен')

    properity_ids = []
    properity_links = []

    local_files = os.listdir('cities/')[1:]
    local_cities_files =[filename.replace('links_', '').replace('.json', '') for filename in os.listdir('cities/')[1:]]
    cities_dict = {k: v for k, v in cities_dict.items() if k not in local_cities_files}
    if len(cities_dict) != 0:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda x: parse_pages_for_links(*x), cities_dict.items()))

        for worker in results:
            ids, links = worker
            properity_ids += ids
            properity_links += links
    
        set_info('все ссылки с городов получены. Нажмите любую кнопку, чтобы перейти к следующему этапу парсинга!')
        input()

    set_info('парсинг страниц недвижимости начался')
    results_files = [filename.replace('.xlsx', '') for filename in os.listdir('results/')]
    for file in local_files:
        city_name = file.replace('links_', '').replace('.json', '')
        if city_name in results_files:
            continue
        with open(os.path.join('cities', file), 'r') as f:
            properties = json.load(f)
            set_info(f'начало процесса парсинга города {city_name}. Всего страниц для парсинга {len(properties)}')
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(tqdm(executor.map(lambda x: parse_page(*x), properties.items()), total=len(properties)))
            
            set_info(f'парсинг страниц недвижимости города {city_name} закончен! Ввод результатов в таблицу')
            df = pd.DataFrame()
            for result in results:
                df = pd.concat([df, pd.DataFrame(result)], ignore_index=True)
            set_info(f'запись результатов в таблицу results/{city_name}.xlsx') 
            df.to_excel(f"results/{city_name}.xlsx")
            set_info('запись в таблицу завершена') 
    
    set_info('парсинг страниц недвижимости закончен')
    
    
    # for result in results:
    #     df = pd.concat([df, pd.DataFrame(result)], ignore_index=True)
    
    # print(df.head(5))
       
    
 

def parse_href(href: str):
    pattern = r'([-\d.]+),([-\d.]+)'
    numbers_match = re.search(pattern=pattern, string=href)
    return numbers_match.group(0)



if __name__ == "__main__":
    main()
    