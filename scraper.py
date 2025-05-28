import requests
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urlparse
import json
import os
from dotenv import load_dotenv

load_dotenv()  # For local testing only

# Airtable config
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME = os.getenv("AIRTABLE_TABLE_ID")

# Airtable fields
FIELD_CATEGORY = "fld86aIQ2aip49mBR"
FIELD_TITLE = "fldTjUp5kghUxk2wx"
FIELD_SUMMARY = "fldipOWgznXkJy1hn"
FIELD_ARTICLE_URL = "fldEjPMXV4rvQWaT7"
FIELD_IMAGE_URL = "fldV56jttpZRxzR0r"
FIELD_PUBLICATION_DATE = "fldlHc103Jtojef4v"
FIELD_AUTHOR = "fld4rLLOIpyeeCxa4"

# Airtable API
api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_NAME)

def normalize_url(url):
    parsed = urlparse(url.strip())
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_article_details(article_url):
    try:
        response = requests.get(article_url)
        soup = BeautifulSoup(response.text, "html.parser")
        pub_date = None
        script_tag = soup.find("script",
                               type="application/ld+json",
                               class_="yoast-schema-graph")
        if script_tag:
            json_data = json.loads(script_tag.string)
            for item in json_data.get("@graph", []):
                if item.get("@type") == "Article" and "datePublished" in item:
                    pub_date = item["datePublished"].split("T")[0]
        author = soup.find("div", class_="author-name")
        return pub_date, author.get_text(strip=True) if author else None
    except Exception as e:
        print(f"⚠️ Error extracting {article_url}: {e}")
        return None, None

# Step 1: Get existing articles
existing_urls = set()
for record in table.all():
    url = record.get("fields", {}).get("Article URL")
    if url:
        existing_urls.add(normalize_url(url))

# Step 2: Scrape new articles
base_url = "https://www.iese.edu/search/articles/"
new_articles = []
max_pages = 5
for page in range(1, max_pages + 1):
    page_url = base_url if page == 1 else f"{base_url}{page}/"
    response = requests.get(page_url)
    if response.status_code != 200:
        break

    soup = BeautifulSoup(response.text, "html.parser")
    boxes = soup.select('div.box-icon')
    if not boxes:
        break

    for box in boxes:
        try:
            title = box.select_one('h3.title-icon').get_text(strip=True)
            summary = box.select_one('p.subtitle-icon').get_text(strip=True)
            raw_url = box.select_one('a.title-link')['href']
            full_url = f"https://www.iese.edu{raw_url}" if raw_url.startswith("/") else raw_url
            article_url = normalize_url(full_url)

            if article_url in existing_urls:
                continue

            category = box.select_one('a.subtitle-link').get_text(strip=True)
            img_tag = box.select_one('a.img-container img')
            image_url = img_tag.get('data-src') or img_tag.get('src')
            pub_date, author = extract_article_details(article_url)

            record = {
                FIELD_CATEGORY: category,
                FIELD_TITLE: title,
                FIELD_SUMMARY: summary,
                FIELD_ARTICLE_URL: article_url,
                FIELD_IMAGE_URL: image_url,
            }
            if pub_date:
                record[FIELD_PUBLICATION_DATE] = pub_date
            if author:
                record[FIELD_AUTHOR] = author

            table.create(record)
            existing_urls.add(article_url)
            new_articles.append({
                "title": title,
                "url": article_url,
                "publication_date": pub_date
            })
            print(f"✅ ADDED: {title}")
        except Exception as e:
            print(f"❌ Error processing: {e}")

# Final output
if new_articles:
    print(f"📦 {len(new_articles)} new article(s) added.")
else:
    print("ℹ️ No new articles found.")
