from flask import Flask
import os

app = Flask(__name__)

@app.route('/')
def run_scraper():
    try:
        os.system("python3 iese_scraper.py")
        return "✅ Scraper ran successfully"
    except Exception as e:
        return f"❌ Error: {e}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=81)
