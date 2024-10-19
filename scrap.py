import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    port=os.getenv('DB_PORT')
)

curr = conn.cursor()

curr.execute("""CREATE TABLE IF NOT EXISTS news (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    href VARCHAR(500) NOT NULL,
    UNIQUE(title, href)
)
""")


URL = "https://www.pravda.com.ua"
page = requests.get(URL)

soup = BeautifulSoup(page.content, 'html.parser')

main_topic = soup.find(class_='article_header')

main_news_link = main_topic.find("a")["href"]

curr.execute("""INSERT INTO news (title, href) VALUES (%s,%s)  ON CONFLICT (title, href) DO NOTHING""", (main_topic.text, URL + main_news_link))


print(f"Main topic is: {main_topic.text}, here is full link: {URL + main_news_link}")

print("\n")

trending_topics = soup.find(class_='article_footer')

trending_topics_ukraine = trending_topics.find_all("a")

for topic in trending_topics_ukraine:
    print(topic.text)
    not_full_url = topic["href"]
    print(f"{URL + not_full_url}")
    print("\n")
    curr.execute("""INSERT INTO news (title, href) VALUES (%s,%s) ON CONFLICT (title, href) DO NOTHING""", (topic.text, URL + not_full_url))


all_topics = soup.find(class_='main_content')

news_within_main = all_topics.find_all(class_='article_header')

for count, news in enumerate(news_within_main):

    if news.find("em"):
        news.find("em").decompose()
    print(news.find("a").text)
    news_link = news.find("a")["href"]
    if news_link.startswith("http"):
        print(news_link)
        curr.execute("""INSERT INTO news (title, href) VALUES (%s,%s) ON CONFLICT (title, href) DO NOTHING""", (news.find("a").text, news_link))
    else:
        print(f"{URL + news_link}")
        sub_page = requests.get((URL + news_link))
        sub_soup = BeautifulSoup(sub_page.content, 'html.parser')
        date_and_author = sub_soup.find(class_='post_time')
        print(date_and_author.text)
        curr.execute(
            """INSERT INTO news (title, href) VALUES (%s,%s) ON CONFLICT (title, href) DO NOTHING""", (news.find("a").text,
                                                                                               URL + news_link))

    print("\n")


conn.commit()
curr.close()
conn.close()
