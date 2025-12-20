# ===============================
# 1. IMPORT
# ===============================
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import sqlite3
from datetime import date
import time
import re
import random

# ===============================
# 2. SELENIUM SETUP
# ===============================
options = webdriver.ChromeOptions()
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--start-maximized")

def create_driver():
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.set_page_load_timeout(30)  # CHỐNG TREO
    return driver

driver = create_driver()

# ===============================
# 3. SQLITE SETUP
# ===============================
conn = sqlite3.connect("goodreads.sqlite")
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS books (
    book_id TEXT PRIMARY KEY,
    title TEXT,
    author TEXT,
    genre TEXT,
    publish_year INTEGER,
    cover_image TEXT,
    book_url TEXT
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS book_metrics_daily (
    book_id TEXT,
    date TEXT,
    avg_rating REAL,
    review_count INTEGER,
    PRIMARY KEY (book_id, date)
)
""")
conn.commit()

# ===============================
# 4. BOOK_ID
# ===============================
def make_book_id(url):
    return "GR_" + url.split("/")[-1].split("-")[0]

# ===============================
# 5. CRAWL CHI TIẾT SÁCH (AN TOÀN)
# ===============================
def crawl_book(book_url):
    try:
        try:
            driver.get(book_url)
        except TimeoutException:
            print("⏱️ Timeout → skip:", book_url)
            return None

        time.sleep(random.uniform(1.2, 2.2))
        soup = BeautifulSoup(driver.page_source, "html.parser")

        title_tag = soup.find("h1")
        if not title_tag:
            return None
        title = title_tag.text.strip()

        author_tag = soup.find("a", class_="ContributorLink")
        author = author_tag.text.strip() if author_tag else None

        rating_tag = soup.find("div", class_="RatingStatistics__rating")
        avg_rating = float(rating_tag.text.strip()) if rating_tag else None

        review_tag = soup.find("span", {"data-testid": "ratingsCount"})
        review_count = int(re.sub(r"[^\d]", "", review_tag.text)) if review_tag else None

        publish_year = None
        for d in soup.find_all("div", class_="FeaturedDetails"):
            m = re.search(r"First published.*?(\d{4})", d.get_text())
            if m:
                publish_year = int(m.group(1))
                break

        cover_tag = soup.find("img", class_="ResponsiveImage")
        cover_image = cover_tag["src"] if cover_tag else None

        return {
            "title": title,
            "author": author,
            "avg_rating": avg_rating,
            "review_count": review_count,
            "publish_year": publish_year,
            "cover_image": cover_image,
            "book_url": book_url
        }

    except WebDriverException:
        print("❌ WebDriver error → skip:", book_url)
        return None

# ===============================
# 6. CÀO NHIỀU THỂ LOẠI
# ===============================
GENRES = [
    "fiction",
    "fantasy",
    "romance",
    "mystery",
    "science-fiction",
    "historical-fiction",
    "thriller",
    "young-adult"
]

MAX_PAGES = 20  # tăng nếu cần nhiều sách hơn

for genre in GENRES:
    print(f"\n========== GENRE: {genre.upper()} ==========")

    for page in range(1, MAX_PAGES + 1):
        list_url = f"https://www.goodreads.com/shelf/show/{genre}?page={page}"

        try:
            driver.get(list_url)
        except TimeoutException:
            print("⏱️ Timeout list page, skip page", page)
            continue

        time.sleep(random.uniform(1.5, 2.5))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        book_links = soup.select("a.bookTitle")

        print(f"📄 Page {page} → {len(book_links)} books")

        for a in book_links:
            book_url = "https://www.goodreads.com" + a["href"]
            book_id = make_book_id(book_url)

            # Check trùng
            cur.execute("SELECT 1 FROM books WHERE book_id = ?", (book_id,))
            if cur.fetchone():
                continue

            data = crawl_book(book_url)
            if not data:
                continue

            # Lưu books
            cur.execute("""
            INSERT OR IGNORE INTO books
            (book_id, title, author, genre, publish_year, cover_image, book_url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                book_id,
                data["title"],
                data["author"],
                genre,
                data["publish_year"],
                data["cover_image"],
                data["book_url"]
            ))

            # Lưu metrics
            cur.execute("""
            INSERT OR IGNORE INTO book_metrics_daily
            (book_id, date, avg_rating, review_count)
            VALUES (?, ?, ?, ?)
            """, (
                book_id,
                str(date.today()),
                data["avg_rating"],
                data["review_count"]
            ))

            conn.commit()
            print("✅ Saved:", data["title"])
            time.sleep(random.uniform(0.8, 1.5))

    # RESET CHROME SAU MỖI GENRE
    driver.quit()
    driver = create_driver()

# ===============================
# 7. KẾT THÚC
# ===============================
driver.quit()
conn.close()
print("\n🎉 DONE! SQLite: goodreads.sqlite")
