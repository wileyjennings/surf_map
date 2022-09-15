import sqlite3
from datetime import datetime

import requests
from bs4 import BeautifulSoup

new_england_map = {
    'Good Harbor Beach': 'https://magicseaweed.com/Good-Harbor-Beach-Surf-Report/9268/',
    'Cape Ann': 'https://magicseaweed.com/Cape-Ann-Surf-Report/370/',
    'Nahant': 'https://magicseaweed.com/Nahant-Surf-Report/1091/',
    'The Wall': 'https://magicseaweed.com/The-Wall-Surf-Report/369/',
    'Hampton Beach': 'https://magicseaweed.com/Hampton-Beach-Surf-Report/2074/',
    'Jenness Beach': 'https://magicseaweed.com/Jenness-Beach-Surf-Report/881/',
    '2nd Beach': 'https://magicseaweed.com/2nd-Beach-Sachuest-Beach-Surf-Report/846/',
    'Narragansett Beach': 'https://magicseaweed.com/Narragansett-Beach-Surf-Report/1103/'
}


def parse_msw_stars(url):
    response = requests.get(url)
    wp = response.text
    soup = BeautifulSoup(wp, 'html.parser')
    star = soup.find(name='ul', class_='rating rating-large clearfix')
    surf_ht = star.find(name='li', class_='rating-text text-dark').getText().strip()
    stars_dark = len(star.findAll(name='li', class_='active'))
    stars_light = len(star.findAll(name='li', class_='inactive'))
    stars_empty = len(star.findAll(name='li', class_='placeholder'))
    return surf_ht, stars_dark, stars_light, stars_empty


def get_data(spot_map: dict, parse_func: callable) -> list:
    """
    :param spot_map: (dict) mapping from spot name or other label to the URL to scrape.
    :param parse_func: (callable) function that accepts a URL and returns a tuple of values to add to the returned data
    :return: (list) a list of tuples containing (spot name, current date, current time, and values from parse func)
    """
    data = []
    now = datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    current_time = now.strftime("%H:%M")
    for spot, url in spot_map.items():
        values = (spot, current_date, current_time, *parse_func(url))
        data.append(values)
    return data


data = get_data(new_england_map, parse_msw_stars)

db_name = 'surf_map.db'
con = sqlite3.connect(db_name)
cur = con.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS msw_stars(spot, date, time, surf_ht, stars_dark, stars_light, stars_empty)")

cur.executemany("INSERT INTO msw_stars VALUES(?, ?, ?, ?, ?, ?, ?)", data)
con.commit()
con.close()
