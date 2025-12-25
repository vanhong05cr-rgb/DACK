# ===============================
# 1. IMPORT
# ===============================
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
import re

# ===============================
# 2. MONGODB SETUP
# ===============================
client = MongoClient("mongodb://localhost:27017/")
db = client["goodreads_final"]
books_col = db["books"]
metrics_col = db["book_metrics_daily"]

metrics_col.create_index([("book_id", 1), ("date", 1)], unique=True)

# ===============================
# 3. UTIL
# ===============================
def make_book_id(url):
    return "GR_" + url.split("/")[-1].split("-")[0]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ===============================
# 4. LẤY COMMENTS (REVIEW TEXT)
# ===============================
def extract_comments(soup, limit=5):
    comments = []
    review_blocks = soup.select("article.ReviewCard span.Formatted")

    for r in review_blocks[:limit]:
        text = r.get_text(" ", strip=True)
        if text:
            comments.append(text)

    return comments

# ===============================
# 5. CRAWL CHI TIẾT (REQUESTS – NHANH)
# ===============================
def crawl_book_fast(book_url, genre):
    try:
        r = requests.get(book_url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1").get_text(strip=True)

        author_tag = soup.find("a", class_="ContributorLink")
        author = author_tag.get_text(strip=True) if author_tag else None

        rating_tag = soup.find("div", class_="RatingStatistics__rating")
        avg_rating = float(rating_tag.get_text(strip=True)) if rating_tag else None

        review_span = soup.find("span", {"data-testid": "ratingsCount"})
        review_count = int(re.sub(r"[^\d]", "", review_span.get_text())) if review_span else None

        cover_tag = soup.find("img", class_="ResponsiveImage")
        cover_image = cover_tag["src"] if cover_tag else None

        publish_year = None
        for d in soup.find_all("div", class_="FeaturedDetails"):
            m = re.search(r"First published.*?(\d{4})", d.get_text())
            if m:
                publish_year = int(m.group(1))
                break

        # ✅ COMMENTS
        comments = extract_comments(soup, limit=5)

        return {
            "book_url": book_url,
            "title": title,
            "author": author,
            "avg_rating": avg_rating,
            "review_count": review_count,
            "publish_year": publish_year,
            "cover_image": cover_image,
            "genres": [genre],
            "comments": comments
        }

    except Exception as e:
        print(" Error:", book_url)
        return None

# ===============================
# 6. SELENIUM SETUP (CHỈ LẤY LINK)
# ===============================
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

# ===============================
# 7. LẤY LINK SÁCH
# ===============================
GENRES = [
    "art", "biography", "business", "chick-lit", "christian", "classics",
    "comics", "contemporary", "cookbooks", "crime", "fantasy", "fiction",
    "graphic-novels", "historical-fiction", "history", "horror",
    "humor", "manga", "memoir", "music", "mystery", "nonfiction",
    "poetry", "psychology", "religion", "romance", "science",
    "science-fiction", "self-help", "suspense", "spirituality",
    "sports", "thriller", "travel", "young-adult"
]

MAX_PAGES = 1
book_urls = set()

for genre in GENRES:
    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.goodreads.com/shelf/show/{genre}?page={page}"
        driver.get(url)
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        links = soup.select("a.bookTitle")

        for a in links:
            book_urls.add("https://www.goodreads.com" + a["href"])

        print(f"📄 {genre} | page {page} → total links: {len(book_urls)}")

driver.quit()
print(f"\n TOTAL BOOK LINKS: {len(book_urls)}")

# ===============================
# 8. GIỚI HẠN CHẠY TRƯỚC 50 SÁCH
# ===============================
LIMIT = 50
book_urls_test = list(book_urls)[:LIMIT]

# ===============================
# 9. ĐA LUỒNG CRAWL + LƯU DB
# ===============================
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(crawl_book_fast, url, "mixed")
        for url in book_urls_test
    ]

    for future in as_completed(futures):
        data = future.result()
        if not data:
            continue

        book_id = make_book_id(data["book_url"])

        # LƯU THÔNG TIN TĨNH
        books_col.update_one(
            {"_id": book_id},
            {"$set": {
                "title": data["title"],
                "author": data["author"],
                "publish_year": data["publish_year"],
                "cover_image": data["cover_image"],
                "book_url": data["book_url"],
                "genres": data["genres"],
                "comments": data["comments"]
            }},
            upsert=True
        )

        # LƯU METRICS THEO NGÀY
        metrics_col.update_one(
            {"book_id": book_id, "date": str(date.today())},
            {"$setOnInsert": {
                "avg_rating": data["avg_rating"],
                "review_count": data["review_count"]
            }},
            upsert=True
        )

        print(" Saved:", data["title"])

print("\n DONE – Crawl 50 books + comments thành công")
