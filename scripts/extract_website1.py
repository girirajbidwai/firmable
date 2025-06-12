import os
import zipfile
import csv
import time
import json
import gzip
import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator
from pyspark.sql import SparkSession

# -----------------------------
# Configuration & Paths
# -----------------------------
WARC_INPUT_CSV = ""
WARC_OUTPUT_DIR = ""
WARC_DOWNLOAD_DIR = ""
ZIP_PATH = ""
EXTRACT_DIR = ""
URL_OUTPUT_DIR = ""

os.makedirs(WARC_DOWNLOAD_DIR, exist_ok=True)
os.makedirs(WARC_OUTPUT_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)
os.makedirs(URL_OUTPUT_DIR, exist_ok=True)

BASE_WARC_URL = "https://data.commoncrawl.org/"

# -----------------------------
# Spark Session
# -----------------------------
spark = SparkSession.builder \
    .appName("CombinedAUExtractor") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# -----------------------------
# WARC Domain Extractor
# -----------------------------
def process_warc(warc_url):
    from tempfile import NamedTemporaryFile
    import traceback

    domains = set()
    try:
        warc_filename = warc_url.split("/")[-1]
        warc_path = os.path.join(WARC_DOWNLOAD_DIR, warc_filename)

        response = requests.get(warc_url, stream=True, timeout=600)
        if response.status_code != 200:
            return []

        with open(warc_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        with gzip.open(warc_path, 'rb') as stream:
            for record in ArchiveIterator(stream):
                if record.rec_type == 'response':
                    url = record.rec_headers.get_header('WARC-Target-URI')
                    if url:
                        parsed = urlparse(url)
                        domain = f"{parsed.scheme}://{parsed.netloc}"
                        if domain.endswith(".au"):
                            domains.add(domain)

        os.remove(warc_path)
        return list(domains)

    except Exception as e:
        print(f"Error processing {warc_url}: {e}")
        print(traceback.format_exc())
        return []

# -----------------------------
# HTML Info Extraction Function
# -----------------------------
def extract_info(html):
    soup = BeautifulSoup(html, "html.parser")
    name = ""
    industry = ""

    if soup.title:
        name = soup.title.get_text(strip=True)

    for meta in soup.find_all("meta"):
        meta_name = meta.get("name", "").lower()
        meta_prop = meta.get("property", "").lower()
        content = meta.get("content", "").strip()

        if not name and meta_name in ["application-name"] or meta_prop in ["og:site_name", "og:title", "twitter:title"]:
            name = content
        if not industry and meta_name in ["industry", "category", "business", "sector"]:
            industry = content

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                data = data[0]
            if isinstance(data, dict):
                name = name or data.get("name", "")
                industry = industry or data.get("industry", data.get("category", ""))
        except:
            continue

    name_keywords = ["company", "organization", "org", "brand", "site", "business"]
    industry_keywords = ["industry", "sector", "field", "type", "category", "business"]

    if not name:
        for tag in soup.find_all(attrs={"class": True}):
            if any(k in tag.get("class", [""])[0].lower() for k in name_keywords):
                name = tag.get_text(strip=True)
                break

    if not industry:
        for tag in soup.find_all(attrs={"class": True}):
            if any(k in tag.get("class", [""])[0].lower() for k in industry_keywords):
                industry = tag.get_text(strip=True)
                break

    return name.strip(), industry.strip()

# -----------------------------
# HTML Parser via Spark Partition
# -----------------------------
def fetch_url_info_partition(iterator):
    results = []
    for row in iterator:
        url = row.strip()
        if not url.startswith("http"):
            url = "http://" + url
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                name, industry = extract_info(resp.text)
                results.append((url, name, industry))
            else:
                results.append((url, "", ""))
        except:
            results.append((url, "", ""))
        time.sleep(0.5)
    return results

# -----------------------------
# Unzip CSV Files
# -----------------------------
def unzip_csv_files():
    with zipfile.ZipFile(ZIP_PATH, 'r') as zipf:
        zipf.extractall(EXTRACT_DIR)
    print(f"✅ Extracted all CSVs to: {EXTRACT_DIR}")

# -----------------------------
# Process Domain Extraction
# -----------------------------
def extract_domains_from_warcs():
    df = spark.read.option("header", "true").csv(WARC_INPUT_CSV)
    warc_urls = df.select("file_name").rdd.map(lambda row: BASE_WARC_URL + row["file_name"])
    results = warc_urls.flatMap(lambda url: process_warc(url)).distinct()
    results.coalesce(1).saveAsTextFile(os.path.join(WARC_OUTPUT_DIR, "au_domains.csv"))
    print("✅ AU domains saved.")

# -----------------------------
# Process Unzipped Domain CSVs
# -----------------------------
def process_all_csvs():
    for csv_file in sorted(os.listdir(EXTRACT_DIR)):
        if csv_file.endswith(".csv") and "domains_" in csv_file:
            input_path = os.path.join(EXTRACT_DIR, csv_file)
            output_path = os.path.join(URL_OUTPUT_DIR, f"parsed_{csv_file}")

            print(f"\n📄 Processing: {csv_file}")
            df = spark.read.text(input_path)
            rdd = df.rdd.repartition(max(1, df.count() // 5))
            result_rdd = rdd.mapPartitions(fetch_url_info_partition)
            result_df = result_rdd.toDF(["URL", "Company Name", "Industry"])
            result_df.write.option("header", "true").csv(output_path)
            print(f"✅ Saved output: {output_path}")

# -----------------------------
# Main
# -----------------------------
def main():
    unzip_csv_files()
    extract_domains_from_warcs()
    process_all_csvs()
    print("\n🎉 All processing complete.")

if __name__ == "__main__":
    main()
