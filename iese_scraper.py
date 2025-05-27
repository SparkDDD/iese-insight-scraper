import requests
from bs4 import BeautifulSoup
from pyairtable import Api
from urllib.parse import urlparse
import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

# Airtable setup
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME = os.getenv("AIRTABLE_TABLE_ID")

# Airtable field IDs
FIELD_CATEGORY = "fld86aIQ2aip49mBR"
FIELD_TITLE = "fldTjUp5kghUxk2wx"
FIELD_SUMMARY = "fldipOWgznXkJy1hn"
FIELD_ARTICLE_URL = "fldEjPMXV4rvQWaT7"
FIELD_IMAGE_URL = "fldV56jttpZRxzR0r"
FIELD_PUBLICATION_DATE = "fldlHc103Jtojef4v"
FIELD_AUTHOR = "fld4rLLOIpyeeCxa4"

# Email setup (matches GitHub Secrets)
SMTP_SERVER = os.getenv("EMAIL_HOST")
SMTP_PORT = int(os.getenv("EMAIL_PORT", 587))
SMTP_USERNAME = os.getenv("EMAIL_HOST_USER")
SMTP_PASSWORD = os.getenv("EMAIL_HOST_PASSWORD")
EMAIL_FROM = SMTP_USERNAME  # From address same as user
EMAIL_TO = os.getenv("EMAIL_TO", "").replace("\n", "").strip()

# Initialize Airtable API
api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_NAME)

def send_email(new_articles):
    if not new_articles:
        print("📭 No new articles to email.")
        return

    sorted_articles = sorted(new_articles, key=lambda x: x.get("publication_date", ""), reverse=True)
    top_articles = sorted_articles[:5]

    html = """
    <html>
    <body>
    <h2>📬 IESE Insight – Latest New Articles</h2>
    <ul>
    """
    for article in top_articles:
        html += f"""
        <li>
            <h3>{article['title']}</h3>
            <p><strong>Summary:</strong> {article['summary']}</p>
            <p><strong>Author:</strong> {article.get('author', 'N/A')}</p>
            <p><strong>Published on:</strong> {article.get('publication_date', 'Unknown')}</p>
            <p><img src="{article.get('image_url', '')}" width="400"/></p>
            <p><a href="{article['url']}">Read more</a></p>
        </li><hr/>
        """

    html += "</ul>"
    if len(sorted_articles) > 5:
        html += f"<p><em>+ {len(sorted_articles) - 5} more new articles available in Airtable.</em></p>"
    html += f"<footer><p>Sent from IESE Insight Scraper on {datetime.now().strftime('%Y-%m-%d %H:%M')}</p></footer>"
    html += "</body></html>"

    # Get environment variables and sanitize
    EMAIL_FROM = os.getenv("EMAIL_FROM", "").strip()
    EMAIL_TO = os.getenv("EMAIL_TO", "").replace("\n", "").strip()
    SMTP_SERVER = os.getenv("SMTP_SERVER", "").strip()
    SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
    SMTP_USERNAME = os.getenv("SMTP_USERNAME", "").strip()
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").strip()

    # Parse multiple recipients
    recipients = [email.strip() for email in EMAIL_TO.split(",") if email.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "IESE Insight – New Articles Update"
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(recipients)

    part = MIMEText(html, "html")
    msg.attach(part)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, recipients, msg.as_string())
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")


def normalize_url(url):
    parsed = urlparse(url.strip())
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")

def extract_article_details(article_url):
    try:
        response = requests.get(article_url)
        soup = BeautifulSoup(response.text, "html.parser")
        pub_date = None
        script_tag = soup.find("script", type="application/ld+json", class_="yoast-schema-graph")
        if script_tag:
            json_data = json.loads(script_tag.string)
            for item in json_data.get("@graph", []):
                if item.get("@type") == "Article" and "datePublished" in item:
                    pub_date = item["datePublished"].split("T")[0]
        author = None
        author_div = soup.find("div", class_="author-name")
        if author_div:
            author = author_div.get_text(strip=True)
        return pub_date, author
    except Exception as e:
        print(f"⚠️ Could not extract details from {article_url}: {e}")
        return None, None

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

base_url = "https://www.iese.edu/search/articles/"
page = 1
new_articles = []

while page <= 5:
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

            pub_date, author = extract_article_details(article_url)

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
            new_articles.append({
                "title": title,
                "summary": summary,
                "url": article_url,
                "image_url": image_url,
                "publication_date": pub_date,
                "author": author
            })
            existing_urls.add(article_url)
            print(f"✅ ADDED: {title} ({pub_date}, {author})")

        except Exception as e:
            print(f"❌ Error processing article: {e}")

    page += 1

print(f"\n📦 Done. {len(new_articles)} new articles added with publication dates and authors.")
send_email(new_articles)
