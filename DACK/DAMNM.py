from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import date
import time, re, random
import concurrent.futures

# =============================== MONGODB ===============================
client = MongoClient("mongodb://localhost:27017/")
db = client["goodreads"]
books_col = db["books"]
metrics_col = db["book_metrics_daily"]
metrics_col.create_index([("book_id", 1), ("date", 1)], unique=True)

# =============================== HELPER ===============================
def make_book_id(url):
    return "GR_" + url.split("/")[-1].split("-")[0]

# =============================== CRAWL CHI TIẾT SÁCH ===============================
def crawl_book(book_url):
    book_id = make_book_id(book_url)
    # --- Tránh crawl trùng lặp ---
    if books_col.find_one({"_id": book_id}):
        print(f"♻️ Skipped (already exists): {book_url}")
        return None
    
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    try:
        driver.get(book_url)
        time.sleep(random.uniform(2, 4))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        title_tag = soup.find("h1", {"data-testid": "bookTitle"})
        title = title_tag.get_text(strip=True) if title_tag else None
        
        author_tag = soup.find("a", {"data-testid": "authorName"})
        author = author_tag.get_text(strip=True) if author_tag else None
        
        rating_tag = soup.find("div", class_="RatingStatistics__rating")
        avg_rating = float(rating_tag.get_text(strip=True)) if rating_tag else None
        
        review_tag = soup.find("span", {"data-testid": "ratingsCount"})
        review_count = int(re.sub(r"[^\d]", "", review_tag.get_text())) if review_tag else None
        
        cover_tag = soup.find("img", class_="ResponsiveImage")
        cover_image = cover_tag["src"] if cover_tag else None
        
        publish_year = None
        for d in soup.find_all("div", class_="FeaturedDetails"):
            match = re.search(r"First published.*?(\d{4})", d.get_text())
            if match:
                publish_year = int(match.group(1))
                break
        
        data = {
            "title": title,
            "author": author,
            "avg_rating": avg_rating,
            "review_count": review_count,
            "publish_year": publish_year,
            "cover_image": cover_image,
            "book_url": book_url
        }
    except Exception as e:
        print(f"⚠️ Error crawl {book_url}: {e}")
        data = None
    driver.quit()
    return data

# =============================== LƯU MONGO ===============================
def save_book(data):
    if not data or not data.get("title"):
        return
    book_id = make_book_id(data["book_url"])
    books_col.update_one(
        {"_id": book_id},
        {"$set": {
            "title": data["title"],
            "author": data["author"],
            "publish_year": data["publish_year"],
            "cover_image": data["cover_image"],
            "book_url": data["book_url"]
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
    print("✅ Saved:", data["title"])

# =============================== LẤY LINK SÁCH ===============================
def get_book_links(genre, page=1):
    """1 driver duy nhất để lấy link"""
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    # options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    
    url = f"https://www.goodreads.com/shelf/show/{genre}?page={page}"
    driver.get(url)
    time.sleep(random.uniform(2,4))
    
    soup = BeautifulSoup(driver.page_source, "html.parser")
    links = []
    # Cập nhật selector mới: sách nằm trong thẻ a.bookTitle
    for a in soup.select("a.bookTitle"):
        href = a.get("href")
        if href and "/book/show/" in href:
            links.append("https://www.goodreads.com" + href.split('?')[0])
    
    driver.quit()
    return list(set(links))

# =============================== MAIN ===============================
def main():
    GENRES = [
        "art", "biography", "business", "chick-lit", "christian", "classics",
        "comics", "contemporary", "cookbooks", "crime", "fantasy", "fiction",
        "graphic-novels", "historical-fiction", "history", "horror",
        "humor", "manga", "memoir", "music", "mystery", "nonfiction",
        "poetry", "psychology", "religion", "romance", "science",
        "science-fiction", "self-help", "suspense", "spirituality",
        "sports", "thriller", "travel", "young-adult"
    ]  
    MAX_PAGES = 20
    MAX_WORKERS = 5
    
    all_links = []
    
    for genre in GENRES:
        for page in range(1, MAX_PAGES+1):
            links = get_book_links(genre, page)
            all_links.extend(links)
            print(f"📄 {genre} page {page} - found {len(links)} books")
    
    all_links = list(set(all_links))  # loại bỏ trùng
    print(f"🔗 Total links: {len(all_links)}")

    # Multi-thread crawl chi tiết sách
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(crawl_book, link) for link in all_links]
        for future in concurrent.futures.as_completed(futures):
            data = future.result()
            save_book(data)
    
    print("🎉 DONE!")

if __name__ == "__main__":
    main()
