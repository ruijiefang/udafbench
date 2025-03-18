import os
import requests
import time
from sys import argv

# GitHub API details



def read_secret():
    with open(argv[1]) as f:
        s = f.read()
    return s.lstrip().rstrip()

GITHUB_TOKEN = read_secret()  
#SEARCH_QUERY = 'import org.apache.spark.sql.expressions.UserDefinedAggregateFunction'
#SEARCH_QUERY = 'import org.apache.spark.sql.expressions.Aggregator'
SEARCH_QUERY = 'org.apache.spark.util.AccumulatorV2'
OUTPUT_DIR = "./spark_udaf_acc"
HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)


def search_github_files(query, per_page=30, max_pages=90):
    """Search GitHub for Java and Scala files containing the query string."""
    files = []
    base_url = "https://api.github.com/search/code"
    print('starting github search..., 30 per page, maxpages=',max_pages)
    for page in range(1, max_pages + 1):
        print('I am on page', page)
        params = {"q": f"{query} extension:java extension:scala", "per_page": per_page, "page": page}
        response = requests.get(base_url, headers=HEADERS, params=params)

        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.json().get('message', 'Unknown error')}")
            break
        print('RESPOSNE GOT')
        results = response.json().get("items", [])
        if not results:
            print('NO MORE RESULTS, RETURN')
            break  # No more results

        files.extend(results)
        time.sleep(10)  # Avoid hitting API rate limits
    print('------- I am done')
    return files

def download_file(file_url, output_path):
    """Download a file from GitHub and save it locally."""
    response = requests.get(file_url, headers=HEADERS)
    time.sleep(2)
    if response.status_code == 200:
        with open(output_path, "w", encoding="utf-8") as file:
            file.write('//' + file_url + '\n')
            file.write(response.text)
        print(f"Saved: {output_path}")
    else:
        print(f"Failed to download {file_url}: {response.status_code}")

def main():
    print("Searching GitHub for matching Java and Scala files...")
    files = search_github_files(SEARCH_QUERY)

    if not files:
        print("No files found.")
        return

    print(f"Found {len(files)} matching files. Downloading...")

    tot=len(files)
    i=0
    for file_info in files:
        print('downloading ', i, ' out of ', tot)
        i+=1
        file_url = file_info["html_url"].replace("github.com", "raw.githubusercontent.com").replace("/blob/", "/")
        file_name = file_info["name"]

        # Only download Java or Scala files
        if not (file_name.endswith(".java") or file_name.endswith(".scala")):
            print(f"Skipping non-Java/Scala file: {file_name}")
            continue
        file_name = str(i) + "_" + file_name
        print('file  downloaded; storing to ', file_name)
        output_path = os.path.join(OUTPUT_DIR, file_name)
        download_file(file_url, output_path)

    print("Download complete.")

if __name__ == "__main__":
    if len(argv) != 2:
        print('ERR: usage: python3 crawler.py <file storing github secret key; on my system, it is on ~/.github_ .>')
    main()

