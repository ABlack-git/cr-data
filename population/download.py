import requests
from bs4 import BeautifulSoup
import re

urls = {
    "k 1.1.2007": "https://www.czso.cz/csu/czso/pocet-obyvatel-v-obcich-ceske-republiky-k-112007-c6b8w53g0a",
    "k 1.1.2008": "https://www.czso.cz/csu/czso/pocet-obyvatel-v-obcich-k-112008-49gzg2k4gw",
    "k 1.1.2009": "https://www.czso.cz/csu/czso/pocet-obyvatel-v-obcich-k-112009-ilog6rq7bi",
}


def get_urls():
    base_url = "https://www.czso.cz"
    start_url = "https://www.czso.cz/csu/czso/pocet-obyvatel-v-obcich-k-112010-dubp0ul6zy"
    page = requests.get(start_url)
    soup = BeautifulSoup(page.content, "html.parser")
    links = soup.find('div', {"id": "archiv-wrapper"}).find_all("a")

    for link in links:
        urls[link.getText()] = base_url + link['href']


def get_excel_url(page_url):
    page = requests.get(page_url)
    soup = BeautifulSoup(page.content, "html.parser")
    table_entries = soup.find("table", {"class": "prilohy-publikace"}).find_all("tr")
    excel_url = None
    for tr in table_entries:
        if re.search(r"obcí s rozšířenou působností", tr.find("span").getText()):
            links = tr.find("td", {"class": "odkazy"}).find_all("a")
            for link in links:
                if re.search(r'Excel', link.getText()):
                    excel_url = link['href']

    return excel_url


def download_excel(excel_url, name):
    r = requests.get(excel_url)
    name = name.replace(" ", "-")
    name = name.replace(".", "-")
    if int(name.split("-")[3]) >= 2014:
        file_name = f'{name}.xlsx'
    else:
        file_name = f'{name}.xls'
    with open(f'raw_data/{file_name}', "wb") as f:
        f.write(r.content)


def download():
    get_urls()
    for name, url in urls.items():
        excel_url = get_excel_url(url)
        download_excel(excel_url, name)


download()
