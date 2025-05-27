import requests
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urlparse
import json
import os

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME = os.getenv("AIRTABLE_TABLE_ID")  # e.g., "IESE Articles"

print(f"🔍 BASE_ID: {AIRTABLE_BASE_ID}")
print(f"🔍 TABLE_ID: {AIRTABLE_TABLE_ID}")

# Airtable field IDs
FIELD_CATEGORY = "fld86aIQ2aip49mBR"
FIELD_TITLE = "fldTjUp5kghUxk2wx"
FIELD_SUMMARY = "fldipOWgznXkJy1hn"
FIELD_ARTICLE_URL = "fldEjPMXV4rvQWaT7"
FIELD_IMAGE_URL = "fldV56jttpZRxzR0r"
FIELD_PUBLICATION_DATE = "fldlHc103Jtojef4v"
FIELD_AUTHOR = "fld4rLLOIpyeeCxa4"

# Initialize Airtable API
api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_NAME)

def normalize_url(url):
    parsed = urlparse(url.strip())
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_article_details(article_url):
    """Extract publication date and author name from the article page."""
    try:
        response = requests.get(article_url)
        soup = BeautifulSoup(response.text, "html.parser")

        # Get publication date from JSON-LD
        pub_date = None
        script_tag = soup.find("script", type="application/ld+json", class_="yoast-schema-graph")
        if script_tag:
            json_data = json.loads(script_tag.string)
            for item in json_data.get("@graph", []):
                if item.get("@type") == "Article" and "datePublished" in item:
                    pub_date = item["datePublished"].split("T")[0]

        # Get author name from <div class="author-name">
        author = None
        author_div = soup.find("div", class_="author-name")
        if author_div:
            author = author_div.get_text(strip=True)

        return pub_date, author

    except Exception as e:
        print(f"⚠️ Could not extract details from {article_url}: {e}")
        return None, None

# Step 1: Load existing article URLs from Airtable
print("📥 Fetching existing article URLs from Airtable...")
existing_urls = set()
try:
    for record in table.all():
        fields = record.get("fields", {})
        url = fields.get("Article URL")
        if url:
            existing_urls.add(normalize_url(url))
    print(f"✅ Found {len(existing_urls)} existing article URLs.")
except Exception as e:
    print(f"❌ Error fetching records: {e}")
    exit(1)

# Step 2: Scrape pages until no more articles
base_url = "https://www.iese.edu/search/articles/"
page = 1
new_articles = 0

while True:
    page_url = base_url if page == 1 else f"{base_url}{page}/"
    print(f"\n🌍 Scraping page {page} → {page_url}")
    response = requests.get(page_url)
    if response.status_code != 200:
        print(f"❌ Error loading page {page}. Status: {response.status_code}")
        break

    soup = BeautifulSoup(response.text, "html.parser")
    boxes = soup.select('div.box-icon')

    if not boxes:
        print(f"🚫 No articles found on page {page}. Stopping.")
        break

    print(f"🔍 Found {len(boxes)} articles.")

    for box in boxes:
        try:
            title = box.select_one('h3.title-icon').get_text(strip=True)
            summary = box.select_one('p.subtitle-icon').get_text(strip=True)
            raw_url = box.select_one('a.title-link')['href']
            full_url = f"https://www.iese.edu{raw_url}" if raw_url.startswith("/") else raw_url
            article_url = normalize_url(full_url)

            if article_url in existing_urls:
                print(f"⏭️ SKIP (duplicate): {title}")
                continue

            category = box.select_one('a.subtitle-link').get_text(strip=True)
            img_tag = box.select_one('a.img-container img')
            image_url = img_tag.get('data-src') or img_tag.get('src')

            # Extract date and author
            pub_date, author = extract_article_details(article_url)

            # Compose Airtable record
            record = {
                FIELD_CATEGORY: category,
                FIELD_TITLE: title,
                FIELD_SUMMARY: summary,
                FIELD_ARTICLE_URL: article_url,
                FIELD_IMAGE_URL: image_url
            }
            if pub_date:
                record[FIELD_PUBLICATION_DATE] = pub_date
            if author:
                record[FIELD_AUTHOR] = author

            table.create(record)
            new_articles += 1
            existing_urls.add(article_url)
            print(f"✅ ADDED: {title} ({pub_date}, {author})")

        except Exception as e:
            print(f"❌ Error processing article: {e}")

    page += 1

print(f"\n📦 Done. {new_articles} new articles added with publication dates and authors.")
