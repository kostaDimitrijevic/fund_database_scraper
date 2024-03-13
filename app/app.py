import json
import logging
import os
import hashlib
import sys
import time
import requests
import pandas as pd
import urllib3
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import date

load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)

PROXY = os.getenv("PROXY", "")
FILE_PATH = os.getenv("FILE_PATH", "")

file_name = "fundDatabase.csv"

proxies = {
    "http": PROXY,
    "https": PROXY
}

base_url = "https://fondswelt.hansainvest.com/en/downloads-and-forms/download-center"
headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Host": "fondswelt.hansainvest.com",
    "Referer": "https://fondswelt.hansainvest.com/de/downloads-und-formulare/download-center",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": "\"Google Chrome\";v=\"119\", \"Chromium\";v=\"119\", \"Not?A_Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"macOS\""
}

# EXAMPLE OF PAYLOAD DATA
payload_data = {
    "draw": 1,  # NUMBER OF REQUESTS
    "start": 0,  # OFFSET USED FOR PAGING
    "length": 100,  # NUMBER OF ROWS
}

document_types = {
    1: "Prospectus",
    2: "Annual report",
    3: "Semi annual report"
}


def check_if_exists(md5_hash, data_df):
    check = data_df["MD5Hash"].isin([md5_hash]).any()
    return check


def scrape_fund_database(payload, data_df):
    result_data = []
    # GET ISIN INFORMATION
    table_page = requests.get(url=base_url, headers=headers, verify=False)
    soup = BeautifulSoup(table_page.text, "html.parser")
    option_tags = soup.find_all(name="option")
    isin_arr = []

    for option in option_tags:
        if option.get("data-isin") is not None:
            isin_arr.append(option.get("data-isin"))

    # GET THE PDFS
    url_for_data = f"https://fondswelt.hansainvest.com/en/download-center/datatable?draw={payload['draw']}&" \
                   f"columns%5B0%5D%5Bdata%5D=0&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&" \
                   f"columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&" \
                   f"columns%5B1%5D%5Bdata%5D=1&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&" \
                   f"columns%5B1%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=2&columns%5B2%5D%5Bname%5D=&" \
                   f"columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&" \
                   f"columns%5B3%5D%5Bdata%5D=3&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D=&" \
                   f"columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=4&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&" \
                   f"columns%5B4%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=5&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&" \
                   f"columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D=&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=0&order%5B0%5D%5Bdir%5D=asc&" \
                   f"start={payload['start']}&length={payload['length']}&search%5Bvalue%5D=&search%5Bregex%5D=false&search%5Bfund%5D=&search%5Bcountry%5D=&_=1710145511914"

    # YOU CAN ADD PROXY ATTRIBUTE IN THIS GET METHOD BY ADDING proxies=proxies, VARIABLE 'proxies' is defined
    response = requests.get(url=url_for_data, headers=headers, verify=False)
    if response.status_code == 200:
        logger.info("Downloading PDF files...")
        try:
            response_data = response.json()
            for index, data in enumerate(response_data["data"]):
                # MAKE DIRECTORY FOR THE PDFS
                isin = isin_arr[payload["start"] + index]
                dir = Path(f'{FILE_PATH}Hansainvest/{isin}')
                dir.mkdir(exist_ok=True)

                for index_file, html in enumerate(data):
                    if index_file > 3:
                        break
                    if html != "":
                        soup = BeautifulSoup(html, "html.parser")
                        pdf_tag = soup.find(name="a")
                        if pdf_tag is None:
                            continue
                        pdf_path = pdf_tag.get("href")
                        if "uploads" not in pdf_path:
                            continue
                        effective_date = soup.find(name="span", attrs={"class": "d-inline-block align-middle fs--1"}).find(name="span").text
                        pdf_file_name = pdf_path.split('/')[-1]
                        pdf_url = f"https://fondswelt.hansainvest.com/{pdf_path}"

                        # YOU CAN ADD PROXY ATTRIBUTE IN THIS GET METHOD BY ADDING proxies=proxies, VARIABLE 'proxies' is defined
                        pdf_response = requests.get(url=pdf_url, stream=True)
                        md5_hash = hashlib.md5(pdf_response.content).hexdigest()

                        if not check_if_exists(md5_hash=md5_hash, data_df=data_df):  # CHECK IN fundDatabase IF ALREADY EXISTS
                            # SAVE THE FILE
                            with open(f'{FILE_PATH}Hansainvest/{isin}/{pdf_file_name}', "wb+") as f:
                                output = {}
                                f.write(pdf_response.content)
                                output["ISIN"] = isin
                                output["DocumentType"] = document_types[index_file]
                                output["EffectiveDate"] = effective_date
                                output["DownloadDate"] = date.today()
                                output["DownloadUrl"] = pdf_url
                                output["FilePath"] = f'{FILE_PATH}Hansainvest/{isin}/{pdf_file_name}'
                                output["MD5Hash"] = md5_hash
                                output["FileSize"] = os.path.getsize(output["FilePath"])
                                result_data.append(output)
                                logger.info(f"Done with the file: {pdf_path}")
                        else:
                            logger.info(f"ALREADY EXISTS: {pdf_path}")
        except json.JSONDecodeError:
            logger.error("RESPONSE IS NOT JSON, BAD REQUESTS!")
        except FileNotFoundError:
            logger.error("FILE NOT FOUND!")
    else:
        logger.error(f"BAD REQUEST! STATUS CODE: {response.status_code}")

    return result_data


def main(file_path):
    if os.path.exists(file_path):
        # If the file exists, read the DataFrame from the file
        df = pd.read_csv(file_path)
    else:
        # If the file doesn't exist, create a new DataFrame
        logger.info("Creating a fundDatabase.csv file...")
        columns = ["ISIN", "DocumentType", "EffectiveDate", "DownloadDate", "DownloadUrl", "FilePath", "MD5Hash", "FileSize"]
        df = pd.DataFrame(columns=columns)
        df.to_csv(file_path, index=False)

    logger.info("Starting scraper...")
    for i in range(0, 2):
        payload_data["draw"] = i + 1
        payload_data["start"] = i * payload_data["length"]
        logger.info(f"Starting index: {payload_data['start']}")
        result = scrape_fund_database(payload=payload_data, data_df=df)
        result_df = pd.DataFrame(result)
        df = pd.concat([df, result_df], axis=0)

    df.to_csv(file_path, index=False)


if __name__ == "__main__":
    before = time.time()
    # MAKE A Hansainvest directory if it doesn't already exist
    try:
        logger.info("Making Hansainvest directory...")
        hansa_dir = Path(f'{FILE_PATH}Hansainvest/')
        hansa_dir.mkdir(exist_ok=False)
    except FileExistsError:
        logger.info("Hansainvest directory already exists!")
        pass
    path = FILE_PATH + file_name
    main(file_path=path)
    after = time.time()
    logger.info(f"Done! It took {(after - before) / 60} minutes.")
