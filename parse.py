from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

import concurrent.futures as pool
import undetected_chromedriver.v2 as uc
import time
import json
import csv

# парсер работает для указанных в нем тегов. Детский мир регулярно меняет некоторые тэги,
# поэтому парсер необходимо регулярно актуализировать перед прогонкой

url = 'https://www.detmir.ru/catalog/index/name/lego/'


def del_spaces(text):
    text = text.replace('&nbsp;', '')
    text = text.replace('&thinsp;', '')
    text = text.replace('₽', '')
    print(text)
    return text.strip()

def get_urls(url: str):
    
    goods_counter = 0
    pages_counter = 1

    urls = []

    # инициируем драйвер
    try:
        driver = uc.Chrome()
        driver.get(f'{url}page/{pages_counter}')
        time.sleep(5)
        
        # собираем ссылки на товары с помощью цикла до 500 товаров. При более масштабном сборе данных можно разбить процесс
        # на части
        while(goods_counter <= 500):
            
            goods_counter += len(driver.find_elements(by=By.XPATH, value='//div[contains(@class, "sw")]/child::*'))
            print(f"Кол-во товаров - {goods_counter}")
            
            # с помощью bs4 собираем вместе все необходимые тэги '<a>' вместе
            soup = BeautifulSoup(driver.page_source, 'lxml')
            current_urls = soup.find('div', class_='sw').find_all('a')

            # из собранных вместе тэгов '<a>' собираем ссылки 
            for item in current_urls:
                item_url = item.get('href')
                urls.append(item_url)

            print(urls)

            pages_counter += 1
            driver.get(f'{url}page/{pages_counter}')
            time.sleep(2)
            
        with open('urls.json', 'w', encoding='utf-8') as file:
            json.dump(list(set(urls)), file, indent=4, ensure_ascii=False)

    except Exception as ex:
        print(ex)
    finally:
        driver.close()
        driver.quit()

# функция будет запускаться многопоточно в функции parse_urls
# в функции парсятся необходимые данные для csv таблицы и вносятся в нее
def open_page(url):

    try:
        opt = Options()
        opt.add_argument("--disable-notifications")
        # opt.add_argument('--headless')
        opt.page_load_strategy = 'none'
        driver = uc.Chrome(chrome_options=opt)
        
        driver.get(url=url)
        
        # предусмотрено отключение уведомлений, если они будут выскакивать
        try:
            alert = driver.switch_to.alert
            alert.dismiss()
            time.sleep(1)
        except Exception as no_action:
            print(no_action)
            
        title = driver.find_element(by=By.XPATH, value='//header/h1').text

        try:
            driver.find_element(by=By.XPATH, value='//div[@class="ro"]/span[contains(text(), "Показать всё")]//ancestor::div[@class="ro"]').click()
            time.sleep(2)
        except:
            pass

        item_id = driver.find_element(by=By.XPATH, value='//table//th[text()="Артикул"]/following-sibling::td').text

        price = ''
        promo_price = ''

        try:
            if len(driver.find_elements(by=By.XPATH, value='//div[@class="oh"]/div[1]/div/*')) > 1:
                price = del_spaces(driver.find_element(by=By.XPATH, value='//div[@class="oh"]/div[1]/div/div[2]/span').text)
                promo_price = del_spaces(driver.find_element(by=By.XPATH, value='//div[@class="oh"]/div[1]/div/div[1]').text)

            else:
                price = del_spaces(driver.find_element(by=By.XPATH, value='//div[@class="oh"]/div[1]/div/div').text) 
                promo_price = 'Нет'
        except:
            print("Товара нет в наличии")
            price = 'Нет в наличии'
            promo_price = 'Нет'
        
            
        with open('lego_goods.csv', 'a', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                (
                    item_id,
                    title,
                    price,
                    promo_price,
                    url
                )
            )

            
    except Exception as ex:
        print(ex)
    finally:
        driver.close()
        driver.quit()

# инициируется чистый csv файл с нужными заголовками и многопоточно запускает
# функцию open_page
def parse_urls():

    # получаем список адресов
    with open('urls.json', 'r', encoding='utf-8') as jsonfile:
        urls = json.load(jsonfile)

    with open('lego_goods.csv', 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(
            (
                'id',
                'title',
                'price',
                'promo_price',
                'url'
            )
        )

    executor = pool.ProcessPoolExecutor(max_workers=3)
    executor.map(open_page, urls)

def main():
    get_urls(url)
    parse_urls()

if __name__ == '__main__':
    main()