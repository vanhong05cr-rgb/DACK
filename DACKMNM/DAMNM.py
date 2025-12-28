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
import hashlib

# ===============================
# 2. MONGODB SETUP
# ===============================
client = MongoClient("mongodb+srv://vanhong05cr:Aptx.4869@cluster0.dxonyik.mongodb.net/Goodreads?retryWrites=true&w=majority")
db = client["Goodreads"]
books_col = db["books"]
reviews_col = db["reviews"]
# ===============================
# 3. UTIL
# ===============================
def make_book_id(url):
    # Lấy phần cuối của URL (VD: 368593.The_4_Hour_Workweek)
    slug = url.split("/")[-1]
    
    # Cắt bỏ phần sau dấu chấm (.) để loại bỏ tên sách kiểu "ID.Title"
    clean_id = slug.split(".")[0]
    
    # Cắt tiếp dấu gạch ngang (-) đề phòng trường hợp link kiểu "ID-Title"
    clean_id = clean_id.split("-")[0]
    
    # Kết quả sẽ chỉ còn lại số ID 
    return "GR_" + clean_id


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}


# ===============================
# 4. LẤY COMMENTS (REVIEW TEXT)
# ===============================
def crawl_reviews(book_id, limit=100):
    comments = []
    page = 1

    while len(comments) < limit:
        url = f"https://www.goodreads.com/book/show/{book_id}/reviews?page={page}"
        r = requests.get(url, headers=HEADERS, timeout=10)

        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        blocks = soup.select("article.ReviewCard span.Formatted")

        if not blocks:
            break

        for b in blocks:
            text = b.get_text(" ", strip=True)
            if text:
                comments.append(text)
                if len(comments) >= limit:
                    break

        page += 1
        time.sleep(0.5)  # tránh bị block
    # print("Reviews:", len(comments), "→", book_url)
    return comments



# ===============================
# 5. CRAWL CHI TIẾT (REQUESTS – NHANH)
# ===============================
def crawl_book_fast(book_url, genres):
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
        book_id = book_url.split("/")[-1].split(".")[0]
        comments = crawl_reviews(book_id, limit=100)


        return {
            "book_url": book_url,
            "title": title,
            "author": author,
            "avg_rating": avg_rating,
            "review_count": review_count,
            "publish_year": publish_year,
            "cover_image": cover_image,
            "genres": genres,
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


MAX_PAGES = 25
book_urls = {}

for genre in GENRES:
    for page in range(1, MAX_PAGES + 1):
        url = f"https://www.goodreads.com/search?q={genre}&search_type=books&page={page}"
        driver.get(url)
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        links = soup.select("a.bookTitle")

        if not links:
            break

        for a in links:
            book_url = "https://www.goodreads.com" + a["href"]
            book_urls.setdefault(book_url, set()).add(genre)

        print(f"📄 {genre} | page {page} → total: {len(book_urls)}")
        
driver.quit()
print(f"\n📚 TOTAL BOOK LINKS: {len(book_urls)}")




# ===============================
# 8. GIỚI HẠN CHẠY
# ===============================
# Để chạy toàn bộ link đã lấy được 
book_urls_test = list(book_urls.items())

# 9. ĐA LUỒNG CRAWL + LƯU DB
# ===============================
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [
        executor.submit(crawl_book_fast, url, list(genres))
        for url, genres in book_urls_test
    ]

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
                "avg_rating": data["avg_rating"],
                "review_count": data["review_count"],
                "review_count_crawled": len(data["comments"]),

                # "comments": data["comments"],
                "last_updated": str(date.today())
            },
            "$addToSet": {
                "genres": {"$each": data["genres"]}
            }},
            upsert=True
        )

        for c in data["comments"]:
            review_hash = hashlib.md5(
                (book_id + c).encode("utf-8")
            ).hexdigest()

            reviews_col.update_one(
                {"_id": review_hash},
                {"$setOnInsert": {
                    "book_id": book_id,
                    "text": c,
                    "genres": data["genres"],
                    "created_at": str(date.today())
                }},
                upsert=True
            )

        print(" Saved:", data["title"])

print("\n DONE – Crawl books + comments thành công")