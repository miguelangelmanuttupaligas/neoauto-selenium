import getopt
import math
import os
import pprint
import re
import sys
import time
import pandas as pd

from urllib.parse import quote_plus

from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located, presence_of_all_elements_located
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
from datetime import datetime
from dateutil import tz
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome import service

ENV_DEFAULT_PATH = r'F:\neoauto-selenium\.env'


def create_list_links(driver, url):
    links_pageable = []
    driver.get(url)
    # driver.implicitly_wait(10)
    value = driver.find_element(By.CLASS_NAME, 's-results__count').text
    cant_results = int(re.findall(r'\d+', value)[0])

    cant_pages = math.ceil(cant_results / 20)
    for page in range(1, cant_pages + 1):
        links_pageable.append(url + f'?page={page}')
    return cant_results, links_pageable


def filter_articles(driver, list_links, user, password, host, database):
    all_ids_and_links = dict()
    filtered_links = []
    for link in list_links:
        driver.get(link)
        # driver.implicitly_wait(10)
        link_articles = chrome_driver.find_elements(By.CLASS_NAME, 'c-results-use__link')
        # links = [link_article.get_attribute('href') for link_article in link_articles]
        for link_article in link_articles:
            link = link_article.get_attribute('href')
            identify = int(link.split('-')[-1])
            all_ids_and_links[identify] = link

    conn = get_connection(user, password, host, database)
    sql = f'SELECT ID FROM data;'
    historical_ids = pd.read_sql(sql, conn)['ID'].values.tolist()
    leaked_links = list(set([i for i in all_ids_and_links.keys()]) - set(historical_ids))
    for ids in leaked_links:
        filtered_links.append(all_ids_and_links[ids])
    return filtered_links


def get_articles_from_link(driver, driver_wait, links):
    autos = []
    for link in links:
        print(f'Processing: {link}')
        driver.get(link)
        date = datetime.now(tz=tz.gettz('America/Lima')).strftime("%Y-%m-%d %H:%M:%S")
        data_auto = dict()
        driver.execute_script("window.scrollTo(0, 0)")
        meta_content = driver_wait.until(presence_of_all_elements_located((By.CLASS_NAME, "idSOrq")))
        content = driver_wait.until(presence_of_all_elements_located((By.CLASS_NAME, "htOtEa")))
        data_auto['Precio'] = driver_wait.until(presence_of_element_located((By.CLASS_NAME, "dYanzN"))).text

        driver.execute_script("window.scrollTo(0, 1000)")
        meta_specs = driver_wait.until(presence_of_all_elements_located((By.CLASS_NAME, "cwUAAs")))
        specs = driver_wait.until(presence_of_all_elements_located((By.CLASS_NAME, "dupRgm")))

        data_auto['ID'] = link.split('-')[-1]
        data_auto['Fecha'] = date

        if (len(meta_content) == 0 and len(content) == 0) or (len(meta_specs) == 0 and len(specs) == 0):
            print('ERROR. Changes class name from content or specs of articles. Renueve class name')
            exit()

        for key, value in zip(meta_content, content):
            data_auto[key.text] = value.text
        for key, value in zip(meta_specs, specs):
            data_auto[key.text] = value.text
        data_auto['URL'] = link
        pprint.pprint(data_auto)
        # Agregar clase Auto y libreria bunch, con modificacion
        # https://stackoverflow.com/questions/1305532/how-to-convert-a-nested-python-dict-to-object/31569634#31569634
        autos.append(data_auto)
    return autos


def get_connection(user, password, host, database):
    return create_engine(f"mysql+pymysql://{user}:%s@{host}/{database}" % quote_plus(password))


def to_save(data_csv, results, user, password, host, database, table):
    conn = get_connection(user, password, host, database)

    columns = ['ID', 'Fecha', 'Precio', 'Año Modelo', 'Kilometraje', 'Transmisión', 'Combustible',
               'Cilindrada', 'Categoría', 'Marca', 'Modelo', 'Año de fabricación',
               'Número de puertas', 'Tracción', 'Color', 'Número cilindros', 'Placa', 'URL'
               ]
    df = pd.DataFrame.from_records(results, columns=columns)
    print(f'Trying export the data to csv file')
    df.to_csv(data_csv, index=False, header=True)
    print(f'Exporting the data in a csv file in path: {data_csv}')
    print(f'Trying save the data to sql table')
    df.to_sql(name=table, con=conn, if_exists='append', index=False, chunksize=1000)
    print(f'Save the data in a sql table in host-database-table respectively: {host}-{database}-{table}')


def chunks(lst, n):
    # Separa la cantidad de enlaces en chunks de igual tamaño
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def prepare_list_process(driver, url, search_csv):
    list_links = []
    cant_results = 0
    df_search = pd.read_csv(search_csv)
    searchs = list(df_search.itertuples(index=False))
    print(f'Calculating quantity of articles to process')
    for value in searchs:
        marca = value[0].replace(' ', '-').lower()
        modelo = value[1].replace(' ', '-').lower()
        compound_url = f'{url}-{marca}-{modelo}'
        temp_cant_results, temp_list_links = create_list_links(driver, compound_url)
        cant_results += temp_cant_results
        list_links += temp_list_links
    print(f'Number of articles found: {cant_results}')
    return list_links


def get_env_parameters(argv: list):
    path = ENV_DEFAULT_PATH
    options, args = getopt.getopt(argv[1:], "e:", ["env ="])
    for name, value in options:
        if name in ['-e', '--env']:
            path = value
    return path


def main_single(driver, driver_wait, url, search_csv, data_csv, user, password, host, database, table):
    data_results: list[dict] = []
    # Ejecucion en un unico hilo
    list_links = prepare_list_process(driver, url, search_csv)
    print(f'Link pages list: {list_links}', sep='\n')
    filtered_links = filter_articles(driver, list_links, user, password, host, database)
    print(f'Total links to process after filtering: {len(filtered_links)}')
    # print(f'Links to process: {filtered_links}')
    if len(filtered_links) != 0:
        # for link in filtered_links:
        data_results += get_articles_from_link(driver, driver_wait, filtered_links)

        print(data_results)
        to_save(data_csv, data_results, user, password, host, database, table)
    else:
        print(f'Process ended due to the absence of links to process')


def initializing_driver_and_wait(driver_path):
    serv = service.Service(executable_path=driver_path)
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--headless')
    driver = webdriver.Chrome(service=serv, options=options)
    driver.delete_all_cookies()
    # driver.maximize_window()
    return driver, WebDriverWait(driver, 20)


if __name__ == '__main__':
    env_path = get_env_parameters(sys.argv)
    print("Importing environment values")
    if load_dotenv(env_path):
        DRIVER_LOCATION = os.getenv('DRIVER_LOCATION')
        URL = os.getenv('URL')
        SEARCH_CSV = os.getenv('SEARCH_CSV')
        DATA_CSV = os.getenv('DATA_CSV')
        USER_DATABASE = os.getenv('USER_DATABASE')
        PASSWORD_DATABASE = os.getenv('PASSWORD_DATABASE')
        HOST_DATABASE = os.getenv('HOST_DATABASE')
        NAME_DATABASE = os.getenv('NAME_DATABASE')
        NAME_TABLE = os.getenv('NAME_TABLE')
    else:
        print(f'ERROR. env file not found in the default path: {env_path}')
        exit()

    chrome_driver, chrome_wait_driver = initializing_driver_and_wait(DRIVER_LOCATION)

    start_time = time.time()
    main_single(chrome_driver, chrome_wait_driver, URL, SEARCH_CSV, DATA_CSV,
                USER_DATABASE, PASSWORD_DATABASE, HOST_DATABASE, NAME_DATABASE, NAME_TABLE)
    print("--- %s seconds ---" % (time.time() - start_time))
