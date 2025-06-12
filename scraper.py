import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import gspread
from dotenv import load_dotenv

# --- Load Replit secret (Google JSON key) ---
load_dotenv()
gc = gspread.service_account_from_dict(json.loads(os.getenv("JSON_KEY")))

# --- Google Sheet Setup ---
SHEET_ID = "1HFN3fmDG927674xXzjtf6mMQEneCOQEkxaAfDGEQONU"
SHEET_NAME = "IESE_Insight"
sheet = gc.open_by_key(SHEET_ID).worksheet(SHEET_NAME)

# --- Field Order in Sheet ---
HEADERS = [
    "Category", "Title", "Publication Date",
    "Author", "Summary", "Article URL", "ImageFile URL"
]

# --- Utility Functions ---
def normalize_url(url):
    parsed = urlparse(url.strip())
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_article_details(url):
    try:
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")
        pub_date, author = None, None

        schema = soup.find("script", type="application/ld+json", class_="yoast-schema-graph")
        if schema:
            graph = json.loads(schema.string).get("@graph", [])
            for item in graph:
                if item.get("@type") == "Article":
                    pub_date = item.get("datePublished", "").split("T")[0]
                    break

        author_tag = soup.find("div", class_="author-name")
        author = author_tag.get_text(strip=True) if author_tag else None
        return pub_date, author
    except Exception as e:
        print(f"⚠️ Failed to extract details from {url}: {e}")
        return None, None

# --- Load existing article URLs to avoid duplicates ---
existing_records = sheet.get_all_records()
existing_urls = {normalize_url(row.get("Article URL", "")) for row in existing_records}

# --- Scrape IESE Articles ---
base_url = "https://www.iese.edu/search/articles/"
new_rows = []
batch = []

for page in range(1, 5):
    url = base_url if page == 1 else f"{base_url}{page}/"
    res = requests.get(url)
    if res.status_code != 200:
        break

    soup = BeautifulSoup(res.text, "html.parser")
    boxes = soup.select("div.box-icon")
    if not boxes:
        break

    for box in boxes:
        try:
            title = box.select_one("h3.title-icon").get_text(strip=True)
            summary = box.select_one("p.subtitle-icon").get_text(strip=True)
            raw_url = box.select_one("a.title-link")["href"]
            full_url = f"https://www.iese.edu{raw_url}" if raw_url.startswith("/") else raw_url
            article_url = normalize_url(full_url)

            if article_url in existing_urls:
                continue

            category = box.select_one("a.subtitle-link").get_text(strip=True)
            img_tag = box.select_one("a.img-container img")
            image_url = img_tag.get("data-src") or img_tag.get("src")

            pub_date, author = extract_article_details(article_url)

            row = [
                category,
                title,
                pub_date or "",
                author or "",
                summary,
                article_url,
                image_url or ""
            ]

            batch.append(row)
            new_rows.append(title)
            existing_urls.add(article_url)

            if len(batch) >= 10:
                sheet.append_rows(batch)
                print(f"📤 Uploaded batch of {len(batch)} articles")
                batch.clear()

            print(f"✅ Queued: {title}")
        except Exception as e:
            print(f"❌ Error parsing article: {e}")

# --- Upload final batch (if any) ---
if batch:
    sheet.append_rows(batch)
    print(f"📤 Uploaded final batch of {len(batch)} articles")

# --- Summary Output ---
if new_rows:
    print(f"📦 {len(new_rows)} new article(s) added.")
else:
    print("ℹ️ No new articles found.")
