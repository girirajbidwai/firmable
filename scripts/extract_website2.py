import os
import csv
import requests
import tempfile
import zipfile
import xml.etree.ElementTree as ET

# --- Constants ---
DATA_URL = []

CSV_OUTPUT = "au_abn_data_chunked.csv"

def download_zip_to_tempfile(url):
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    print("📥 Downloading ZIP in chunks...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
            temp_file.write(chunk)
    temp_file.close()
    return temp_file.name

def process_abn_xml(zip_path, csv_path):
    with zipfile.ZipFile(zip_path, 'r') as zipf, open(csv_path, "w", newline="", encoding="utf-8") as out_csv:
        writer = csv.writer(out_csv)
        writer.writerow(["ABN", "Entity Name", "Postcode", "State"])  # CSV header

        for xml_name in zipf.namelist():
            if not xml_name.endswith(".xml"):
                continue  # Skip non-XML files

            print(f"🔄 Processing {xml_name}...")
            with zipf.open(xml_name) as xml_file:
                for event, elem in ET.iterparse(xml_file, events=("end",)):
                    if elem.tag.endswith("ABR"):  # Change this to correct root tag
                        abn = elem.findtext(".//ABN")
                        name = elem.findtext(".//EntityName")
                        postcode = elem.findtext(".//Postcode")
                        state = elem.findtext(".//State")

                        if abn:
                            writer.writerow([abn, name, postcode, state])
                        elem.clear()

    print("✅ Done. Saved to", csv_path)

def main():
    zip_path = download_zip_to_tempfile(DATA_URL)
    try:
        process_abn_xml(zip_path, CSV_OUTPUT)
    finally:
        os.remove(zip_path)

if __name__ == "__main__":
    main()
