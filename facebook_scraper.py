import csv
import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import re
import locale
import schedule

load_dotenv()

FB_EMAIL = os.getenv('FB_EMAIL')
FB_PASSWORD = os.getenv('FB_PASSWORD')

logging.basicConfig(
    filename='facebook_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

all_content = []
scrolls = 15

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['social_data']
collection = db['facebook_posts']

# Set locale for handling French month names
locale.setlocale(locale.LC_TIME, "fr_FR.utf8")

def create_csv(data, file_name):
    headers = ['postText', 'postDate', 'comments']
    try:
        with open(file_name, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in data:
                writer.writerow({
                    'postText': row['postText'],
                    'postDate': row['postDate'],
                    'comments': ', '.join(row['comments'])
                })
        logging.info(f"CSV file '{file_name}' created successfully.")
    except Exception as e:
        logging.error(f"Error creating CSV file: {e}")

def save_to_mongo(data, db_name, collection_name):
    try:
        db = client[db_name]
        collection = db[collection_name]
        if data:
            collection.insert_many(data)
            logging.info(f"Saved {len(data)} posts to MongoDB collection '{collection_name}' in database '{db_name}'.")
        else:
            logging.warning("No data to save to MongoDB.")
    except Exception as e:
        logging.error(f"Error saving to MongoDB: {e}")

def scroll_down(driver):
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
    except Exception as e:
        logging.error(f"Scrolling failed: {e}")

def convert_relative_date(raw_date):
    now = datetime.now()
    try:
        # Relative dates
        if "h" in raw_date:  # Hours ago
            hours_ago = int(re.search(r'(\d+)', raw_date).group(1))
            return (now - timedelta(hours=hours_ago)).isoformat()
        elif "min" in raw_date:  # Minutes ago
            minutes_ago = int(re.search(r'(\d+)', raw_date).group(1))
            return (now - timedelta(minutes=minutes_ago)).isoformat()

        # Absolute dates in the format '6 juin à 10:30'
        match = re.search(r'(\d{1,2}) (\w+) à (\d{1,2}:\d{2})', raw_date)
        if match:
            day, month_name, time_str = match.groups()
            month_number = datetime.strptime(month_name, "%B").month
            post_datetime = datetime(now.year, month_number, int(day), *map(int, time_str.split(":")))
            return post_datetime.isoformat()

        # Fallback for unrecognized date formats
        return raw_date
    except Exception as e:
        logging.warning(f"Error parsing date '{raw_date}': {e}")
        return raw_date


def get_all_posts(driver):
    posts_data = []
    try:
        WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, "div[role='feed'] > div")))
        posts = driver.find_elements(By.CSS_SELECTOR, "div[role='feed'] > div")
        logging.info(f"Found {len(posts)} posts.")

        for index, post in enumerate(posts):
            try:
                post_text = driver.execute_script(
                    "return arguments[0].querySelector('[data-ad-preview=\"message\"]')?.innerText || '';",
                    post
                )

                # Modify the post date extraction to correctly target the time link
                post_date_str = driver.execute_script(
                    "return arguments[0].querySelector('a[aria-label]')?.getAttribute('aria-label') || '';",
                    post
                )

                post_date = convert_relative_date(post_date_str)

                # Extracting commenter's name using the provided selector
                commenter_elements = post.find_elements(By.CSS_SELECTOR, "span.x193iq5w.xeuugli.x13faqbe.x1vvkbs.x1xmvt09.x1lliihq.x1s928wv.xhkezso.x1gmr53x.x1cpjm7i.x1fgarty.x1943h6x.x4zkp8e.x676frb.x1nxh6w3.x1sibtaa.x1s688f.xzsf02u")
                commenters = [commenter.text.strip() for commenter in commenter_elements if commenter.text.strip()]

                # Extracting comment text using the provided selector
                comment_elements = post.find_elements(By.CSS_SELECTOR, "div[dir='auto'][style='text-align: start;']")
                comments = [comment.text.strip() for comment in comment_elements if comment.text.strip()]

                if post_text and len(post_text) > 20:
                    posts_data.append({
                        'postText': post_text,
                        'postDate': post_date,
                        'comments': comments,
                        'commenters': commenters
                    })
                    logging.info(f"Post {index + 1} collected: {post_text[:50]}...")

            except Exception as e:
                logging.error(f"Error retrieving post data for post {index + 1}: {e}")
    except Exception as e:
        logging.error(f"Error retrieving posts: {e}")
    return posts_data

def run():
    options = Options()
    options.add_argument("--start-maximized")
    driver_service = Service('C:\\Users\\msi\\Desktop\\chromedriver-win64\\chromedriver-win64\\chromedriver.exe')
    driver = webdriver.Chrome()

    try:
        driver.get('https://www.facebook.com/login')
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, 'email'))).send_keys(FB_EMAIL)
        time.sleep(5)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.NAME, 'pass'))).send_keys(FB_PASSWORD)
        time.sleep(5)
        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.NAME, 'login'))).click()
        WebDriverWait(driver, 20).until(EC.url_contains('facebook.com'))
        group_url = "https://www.facebook.com/groups/310327396937461"
        driver.get(group_url)
        time.sleep(5)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']")))

        processed_posts = set()
        no_new_posts_counter = 0
        for _ in range(scrolls):
            posts = get_all_posts(driver)
            new_posts = [post for post in posts if post['postText'] not in processed_posts]

            if not new_posts:
                no_new_posts_counter += 1
                if no_new_posts_counter > 2:
                    logging.info("No new posts found. Stopping scrolling.")
                    break
            else:
                no_new_posts_counter = 0

            for post in new_posts:
                processed_posts.add(post['postText'])
            all_content.extend(new_posts)

            logging.info(f"Added {len(new_posts)} new posts. Total: {len(all_content)}")
            scroll_down(driver)

        # Save collected posts
        if all_content:
            create_csv(all_content, 'facebook_group_posts.csv')
            save_to_mongo(all_content, db_name='ppp', collection_name='facebook_posts')
            logging.info("Scraping and saving complete.")
        else:
            logging.warning("No posts collected.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        driver.quit()

# Scheduling the task to run every day at 10 AM
schedule.every().day.at("13:46").do(run)

if __name__ == "__main__":
    while True:
        schedule.run_pending()  # Run the scheduled task if it's time
        time.sleep(15)  # Sleep for a minute before checking again