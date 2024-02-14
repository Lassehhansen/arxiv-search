### CREDIT GOES TO: pranftw @ https://github.com/pranftw/openreview_scraper/tree/master
### I adjusted the script to fit my specific needs for output
### Notice that papers after 2019 cannot be scraped atm

import requests
import argparse
import json
from tqdm import tqdm
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
import os

# Initializing argparse
parser = argparse.ArgumentParser(description='Script to scrape NeurIPS Papers')

parser.add_argument('-start', action="store", default=1987, dest="start_year", type=int, help='The start year to scrape the papers')
parser.add_argument('-end', action="store", default=2023, dest="end_year", type=int, help='The end year to scrape the papers')
parser.add_argument('-folder', action="store", default="data", dest="folder_path", type=str, help='Folder to save the scraped data')
parser.add_argument('-filename', action="store", default="neurIPS_papers.jsonl", dest="filename", type=str, help='Filename for the output JSONL file')
arguments = parser.parse_args()

# Argparse conditions
if arguments.start_year < 1987 or arguments.start_year > 2023:
    raise ValueError("Please enter a valid start year. Possible values are [1987, 2023].")

if arguments.end_year < 1987 or arguments.end_year > 2023:
    raise ValueError("Please enter a valid end year. Possible values are [1987, 2023].")

if arguments.start_year > arguments.end_year:
    raise ValueError("Start year shouldn't be greater than end year.")

# Constants
BASE_URL = "https://papers.nips.cc/paper/"
PARSER = 'lxml'
HEADERS = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36",
}
start_year = arguments.start_year
end_year = arguments.end_year
papers = []
paper_authors = []


def get_conference_url(start_year, end_year):
    """Return all the URLs of conferences between start_year and end_year"""

    conferences = []
    print("Preparing data...")
    for year in tqdm(range(start_year, end_year+1)):
        year_url = BASE_URL + str(year)
        conferences.append({"URL": year_url})
    return conferences


def get_all_hashes(url):
    """
        Context: The NeurIPS website follow a structured pattern by maintaining a hash for each paper.

        Return all the hashes for a particular year.
    """
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, PARSER)

            hashes = []
            for li in soup.find("div", class_="container-fluid").find_all("li"):
                paper_url = li.a.get('href')
                paper_hash = paper_url.split("/")[-1].split("-")[0]
                hashes.append(paper_hash)
            return hashes
        else:
            print("Couldn't complete the request.")
            return False
    except ConnectionError as error:
        print(error)


def scrap_paper_and_authors(year_url, hashes):
    """Scrap papers and authors using extracted hashes"""

    for paper_hash in tqdm(hashes):
        paper_url = year_url + "/file/" + paper_hash + "-Metadata.json"
        try:
            response = requests.get(paper_url, headers=HEADERS)
            if response.status_code == 200:
                try:
                    doc = response.json()
                except json.JSONDecodeError:
                    print(f"Invalid JSON response for URL: {paper_url}")
                    continue

                # Extracting paper
                paper = {
                    'source_id': doc.get('sourceid'),
                    'year': year_url.split("/")[-1],
                    'title': doc.get('title'),
                    'abstract': doc.get('abstract'),
                    'full_text': doc.get('full_text')
                }
                papers.append(paper)

                # Extracting authors from a paper
                for author in doc.get('authors', []):
                    author_details = {
                        'source_id': doc.get('sourceid'),
                        'first_name': author.get('given_name'),
                        'last_name': author.get('family_name'),
                        'institution': author.get('institution')
                    }
                    paper_authors.append(author_details)
            else:
                print(f"Failed to get response for URL: {paper_url}")
        except ConnectionError as error:
            print(error)



def save_jsonl(file_name, data, folder_path):
    """Save data in JSONL format in the specified folder."""
    # Check if the folder exists, create it if not
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'w') as file:
        for item in data:
            json_record = json.dumps(item, ensure_ascii=False)
            file.write(json_record + '\n')
    print(f"Successfully saved {file_name} in '{folder_path}' folder")


# Main execution logic
conferences = get_conference_url(start_year, end_year)

papers = []
for year in conferences:
    hashes = get_all_hashes(year["URL"])
    if hashes:
        scrap_paper_and_authors(year["URL"], hashes)

if papers:
    # Transforming to the required JSONL format
    jsonl_data = []
    for paper in papers:
        jsonl_record = {
            "text": paper["full_text"],
            "meta": {
                "source_id": paper["source_id"],
                "year": paper["year"],
                "title": paper["title"],
                "abstract": paper["abstract"]
            }
        }
        jsonl_data.append(jsonl_record)

    save_jsonl(arguments.filename, jsonl_data, arguments.folder_path)
else:
    print("No data to save!")
