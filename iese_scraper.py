
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
TABLE_NAME = os.getenv("AIRTABLE_TABLE_ID")  # e.g., "IESE Articles"

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

# Email setup
EMAIL_FROM = os.getenv("EMAIL_FROM")
EMAIL_TO = os.getenv("EMAIL_TO")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

def send_email(new_articles):
    if not new_articles:
        print("📭 No new articles to email.")
        return

    sorted_articles = sorted(
        new_articles,
        key=lambda x: x.get("publication_date", "0000-00-00"),
        reverse=True
    )
    top_articles = sorted_articles[:5]

    html = "<h2>📬 IESE Insight – Latest New Articles</h2><hr>"
    for article in top_articles:
        title = article.get("title", "")
        summary = article.get("summary", "")
        url = article.get("url", "")
        image = article.get("image_url", "")
        author = article.get("author", "N/A")
        pub_date = article.get("publication_date", "Unknown")

        html += f"""
        <div style="margin-bottom:20px;">
            <h3><a href="{url}">{title}</a></h3>
            <p><strong>By:</strong> {author} | <strong>Published:</strong> {pub_date}</p>
            <img src="{image}" style="max-width:300px;"><br>
            <p>{summary}</p>
        </div>
        <hr>
        """

    if len(sorted_articles) > 5:
        html += f"<p><em>+ {len(sorted_articles) - 5} more new articles available in Airtable.</em></p>"

    html += f"""
    <p style="font-size:small;color:gray;">
        You received this update from the IESE Insight Scraper on {datetime.now().strftime('%Y-%m-%d %H:%M')}.
    </p>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "🆕 IESE Insight – New Articles Update"
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        print("✅ Email sent successfully.")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
