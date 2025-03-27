import os
import time
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient

# --- Configurazione Database MongoDB Atlas ---
MONGO_URI = "mongodb+srv://francocico17:<Romaroma17!>@cluster0.ekcfvia.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["central_banks"]
collection = db["speeches"]

# --- Configurazione Selenium ---
options = Options()
options.add_argument("--headless")  # Browser invisibile
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# --- Funzione per inizializzare il WebDriver ---
def get_driver():
    # Usa ChromeDriverManager per installare e gestire il driver
    driver_path = ChromeDriverManager().install()  # Ottieni il percorso del chromedriver
    service = Service(driver_path)
    try:
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        print(f"Errore durante l'inizializzazione del WebDriver: {e}")
        return None

# --- Funzione per attendere elementi ---
def wait_for_element(driver, xpath, timeout=10):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.XPATH, xpath)))

# --- Estrai il numero totale di pagine ---
def get_total_pages(driver):
    try:
        driver.get("https://www.bis.org/cbspeeches/index.htm?m=60&fromDate=01%2F01%2F2025")
        total_pages_element = wait_for_element(driver, '//*[@id="cbspeeches_list"]/div/div[2]/nav/div/div[2]/div/div[2]/span')
        return int(total_pages_element.text.strip().split()[-1])
    except Exception as e:
        print(f"Errore nell'estrazione del numero di pagine: {e}")
        return 1

# --- Estrai i link ai discorsi ---
def get_speech_links(driver, page):
    url = f"https://www.bis.org/cbspeeches/index.htm?m=60&fromDate=01%2F01%2F2025&page={page}"
    driver.get(url)
    time.sleep(2)
    
    links = []
    index = 1
    while True:
        try:
            link_element = driver.find_element(By.XPATH, f'//*[@id="cbspeeches_list"]/div/table/tbody/tr[{index}]/td[2]/div/div[1]/a')
            links.append(link_element.get_attribute("href"))
            index += 1
        except:
            break
    return links

# --- Estrai titolo e contenuto del discorso ---
def extract_speech(url):
    driver = get_driver()
    if driver is None:
        print(f"Impossibile avviare il driver per l'URL: {url}")
        return

    driver.get(url)
    time.sleep(2)
    
    try:
        title = wait_for_element(driver, '//*[@id="center"]/h1').text
        overview = wait_for_element(driver, '//*[@id="extratitle-div"]/p[1]').text
        data_pubblicazione = wait_for_element(driver, '//*[@id="center"]/div[2]/div[2]/div[1]/div').text
        content = wait_for_element(driver, '//*[@id="cmsContent"]').text
        author = wait_for_element(driver, '//*[@id="authorboxgrp"]/div/a/div/div/div[1]').text

        speech_data = {
            "title": title,
            "overview": overview,
            "data_pubblicazione": data_pubblicazione,
            "content": content,
            "author": author,
            "url": url
        }

        collection.insert_one(speech_data)  # Salva su MongoDB
        print(f"Salvato: {title}")
    
    except Exception as e:
        print(f"Errore nell'estrazione: {e}")
    
    finally:
        driver.quit()

# --- Avvio dello scraping concorrente ---
if __name__ == "__main__":
    main_driver = get_driver()
    if main_driver is None:
        print("Impossibile avviare il driver principale!")
        exit()

    total_pages = get_total_pages(main_driver)
    main_driver.quit()

    print(f"Trovate {total_pages} pagine.")

    all_links = []
    for page in range(1, total_pages + 1):
        driver = get_driver()
        if driver is not None:
            all_links.extend(get_speech_links(driver, page))
            driver.quit()

    print(f"Totale discorsi trovati: {len(all_links)}")

    # Avvia scraping concorrente (5 thread)
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(extract_speech, all_links)

    print("Scraping completato!")
