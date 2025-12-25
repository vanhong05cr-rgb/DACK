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

reviews_col = db["book_reviews"]

reviews_col.create_index(
    [("book_id", 1), ("review_id", 1)],
    unique=True
)

# ===============================
# 3. UTIL
# ===============================
def make_book_id(url):
    return "GR_" + url.split("/")[-1].split("-")[0]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# ===============================
# 4. CRAWL CHI TIẾT (REQUESTS – NHANH)
# ===============================
def crawl_book_fast(book_url, genre):
    try:
        r = requests.get(book_url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        title = soup.find("h1").text.strip()

        author_tag = soup.find("a", class_="ContributorLink")
        author = author_tag.text.strip() if author_tag else None

        rating_tag = soup.find("div", class_="RatingStatistics__rating")
        avg_rating = float(rating_tag.text.strip()) if rating_tag else None

        review_span = soup.find("span", {"data-testid": "ratingsCount"})
        review_count = int(re.sub(r"[^\d]", "", review_span.text)) if review_span else None

        cover_tag = soup.find("img", class_="ResponsiveImage")
        cover_image = cover_tag["src"] if cover_tag else None

        publish_year = None
        for d in soup.find_all("div", class_="FeaturedDetails"):
            m = re.search(r"First published.*?(\d{4})", d.get_text())
            if m:
                publish_year = int(m.group(1))
                break

        return {
            "book_url": book_url,
            "title": title,
            "author": author,
            "avg_rating": avg_rating,
            "review_count": review_count,
            "publish_year": publish_year,
            "cover_image": cover_image,
            "genres": [genre]
        }

    except Exception as e:
        print("❌ Error:", book_url)
        return None
def crawl_reviews_selenium(driver, book_url, book_id, max_reviews=5):
    reviews = []

    try:
        driver.get(book_url)
        wait = WebDriverWait(driver, 10)

        # chờ review mới
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "article[data-testid='review']")
        ))

        review_cards = driver.find_elements(
            By.CSS_SELECTOR,
            "article[data-testid='review']"
        )

        for card in review_cards[:max_reviews]:

            review_id = card.get_attribute("data-review-id")

            # username
            try:
                user_name = card.find_element(
                    By.CSS_SELECTOR,
                    "a[data-testid='reviewer-name']"
                ).text.strip()
            except:
                user_name = None

            # rating
            try:
                rating_label = card.find_element(
                    By.CSS_SELECTOR,
                    "span[aria-label*='stars']"
                ).get_attribute("aria-label")
                rating = int(rating_label[0])
            except:
                rating = None

            # show more
            try:
                show_more = card.find_element(
                    By.CSS_SELECTOR,
                    "button[data-testid='show-more-review-text']"
                )
                driver.execute_script("arguments[0].click();", show_more)
                time.sleep(0.3)
            except:
                pass

            # review text
            try:
                review_text = card.find_element(
                    By.CSS_SELECTOR,
                    "span[data-testid='review-text']"
                ).text.strip()
            except:
                review_text = None

            # date
            try:
                review_date = card.find_element(By.TAG_NAME, "time").text
            except:
                review_date = None

            reviews.append({
                "book_id": book_id,
                "review_id": review_id,
                "user_name": user_name,
                "rating": rating,
                "review_text": review_text,
                "review_date": review_date,
                "crawl_date": str(date.today())
            })

        return reviews

    except Exception as e:
        print("⚠️ No reviews / UI changed:", book_url)
        return []

    

# ===============================
# 5. SELENIUM SETUP (CHỈ LẤY LINK)
# ===============================
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

# ===============================
# 6. LẤY LINK SÁCH
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

MAX_PAGES = 1  # mỗi genre
book_urls = set()  # set để tránh trùng

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
print(f"\n🔗 TOTAL BOOK LINKS: {len(book_urls)}")
review_driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

with ThreadPoolExecutor(max_workers=15) as executor:
    futures = [executor.submit(crawl_book_fast, url, genre) for url in book_urls]

    for future in as_completed(futures):
        data = future.result()
        if not data:
            continue

        book_id = make_book_id(data["book_url"])

        books_col.update_one(
            {"_id": book_id},
            {"$set": {
                "title": data["title"],
                "author": data["author"],
                "publish_year": data["publish_year"],
                "cover_image": data["cover_image"],
                "book_url": data["book_url"],
                "genres": {"$each": data["genres"]}
            }},
            upsert=True
        )

        metrics_col.update_one(
            {"book_id": book_id, "date": str(date.today())},
            {"$setOnInsert": {
                "avg_rating": data["avg_rating"],
                "review_count": data["review_count"]
            }},
            upsert=True
        )

        # 👉 GẮN REVIEW Ở ĐÂY
        reviews = crawl_reviews_selenium(
            review_driver,
            data["book_url"],
            book_id,
            max_reviews=3
        )

        for review in reviews:
            reviews_col.update_one(
                {
                    "book_id": review["book_id"],
                    "review_id": review["review_id"]
                },
                {"$setOnInsert": review},
                upsert=True
            )

        print("✅ Saved book + reviews:", data["title"])
        time.sleep(1)
