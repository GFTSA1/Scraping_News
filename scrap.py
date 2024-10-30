import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os
import psycopg2

load_dotenv()

dsn = f"host={os.getenv('DB_HOST')} dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} password={os.getenv('DB_PASSWORD')} port={os.getenv('DB_PORT')}"


class Storage:
    """
    1. context manager interface
    2. search Authors by name
    3. Create Authors
    4. Save news
    """

    def __init__(self, dsn):
        self.dsn = dsn
        self.connection = None

    def __enter__(self):
        self.connection = psycopg2.connect(self.dsn)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()

    def drop_database(self):
        query = """
        DROP TABLE IF EXISTS authors CASCADE;
        DROP TABLE IF EXISTS news CASCADE;"""
        with self.connection.cursor() as cursor:
            cursor.execute(query)

    def check_if_base_is_created(self):
        query = """
            CREATE TABLE IF NOT EXISTS news (
                id SERIAL PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                href VARCHAR(500) NOT NULL,
                UNIQUE(title, href)
        );
            CREATE TABLE IF NOT EXISTS authors (
                id SERIAL PRIMARY KEY,
                author_name VARCHAR(255) NOT NULL,
                author_last_name VARCHAR(255) NOT NULL UNIQUE
        );
            CREATE TABLE IF NOT EXISTS news_authors (
                news_id INTEGER NOT NULL,
                author_id INTEGER NOT NULL,
                PRIMARY KEY (news_id, author_id),
                FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE,
                FOREIGN KEY (author_id) REFERENCES authors(id) ON DELETE CASCADE
        );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)

    def check_if_author_is_in_base(self, author_name, author_last_name):
        query_check = """
            SELECT EXISTS (
                SELECT 1
                FROM authors
                WHERE author_name = %s AND author_last_name = %s
            );
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query_check, (author_name, author_last_name))
            if cursor.fetchone()[0] is False:
                query = """
                INSERT INTO authors (author_name, author_last_name) VALUES (%s, %s)
                """
                cursor.execute(query, (author_name, author_last_name))

    def save_parsed_data(self, parsed_list):
        authors_to_insert = []
        news_to_insert = []
        news_authors_to_insert = []

        with self.connection.cursor() as cursor:
            for item in parsed_list:
                title = item["title"]
                href = item["href"]
                authors = item["author"]

                news_to_insert.append((title, href))

                for author in authors:
                    first_name = author["first_name"]
                    last_name = author["last_name"]

                    authors_to_insert.append((first_name, last_name))

                    news_authors_to_insert.append((title, href, first_name, last_name))

            cursor.executemany(
                """
                INSERT INTO authors (author_name, author_last_name)
                VALUES (%s, %s)
                ON CONFLICT (author_last_name) DO NOTHING;
            """,
                authors_to_insert,
            )

            cursor.executemany(
                """
                INSERT INTO news (title, href)
                VALUES (%s, %s)
                ON CONFLICT (title, href) DO NOTHING;
            """,
                news_to_insert,
            )

            author_id_map = {}
            news_id_map = {}

            cursor.execute("SELECT id, author_name, author_last_name FROM authors;")
            for row in cursor.fetchall():
                author_id_map[(row[1], row[2])] = row[0]

            cursor.execute("SELECT id, title, href FROM news;")
            for row in cursor.fetchall():
                news_id_map[(row[1], row[2])] = row[0]

            for item in news_authors_to_insert:
                title, href, first_name, last_name = item
                author_id = author_id_map.get((first_name, last_name))
                news_id = news_id_map.get((title, href))

                if author_id and news_id:
                    cursor.execute(
                        """
                        INSERT INTO news_authors (news_id, author_id)
                        VALUES (%s, %s)
                        ON CONFLICT (news_id, author_id) DO NOTHING;
                    """,
                        (news_id, author_id),
                    )


class Parser:
    """
    1. Parse news -> list[
        dict[
        'title': 'foo',
        'href': 'foo',
        'author': {
            'first_name': 'foo',
            'last_name': 'foo',
        }
        ]
    ]
    """

    def parse_news(self):
        URL = "https://www.pravda.com.ua"
        page = requests.get(URL)
        soup = BeautifulSoup(page.content, "html.parser")
        parsed_list = []

        main_topic = soup.find(class_="article_header")
        main_news_link = main_topic.find("a")["href"]
        if not main_news_link.startswith("http") or not main_news_link.startswith(
            "/columns"
        ):
            main_news_url = URL + main_news_link
            sub_page = requests.get(main_news_url)
            sub_soup = BeautifulSoup(sub_page.content, "html.parser")
            date_and_author = sub_soup.find(class_="post_time")

            list_of_authors_and_time = date_and_author.text.split("—")

            authors_list = []
            authors = list_of_authors_and_time[0].split(",")
            for author in authors:
                if author:
                    name_parts = author.split()
                    author_name = name_parts[0]
                    author_last_name = name_parts[1] if len(name_parts) > 1 else ""
                    authors_list.append(
                        {
                            "first_name": author_name,
                            "last_name": author_last_name,
                        }
                    )

        parsed_list.append(
            {"title": main_topic.text, "href": main_news_url, "author": authors_list}
        )

        trending_topics = soup.find(class_="article_footer")
        trending_topics_div = trending_topics.find_all("a")

        for topic in trending_topics_div:
            if not topic["href"].startswith("http") or not topic["href"].startswith(
                "http"
            ):
                trending_news_url = URL + topic["href"]
                sub_page = requests.get(trending_news_url)
                sub_soup = BeautifulSoup(sub_page.content, "html.parser")
                date_and_author = sub_soup.find(class_="post_time")

                list_of_authors_and_time = date_and_author.text.split("—")

                authors_list = []
                authors = list_of_authors_and_time[0].split(",")
                for author in authors:
                    if author:
                        name_parts = author.split()
                        author_name = name_parts[0]
                        author_last_name = name_parts[1] if len(name_parts) > 1 else ""
                        authors_list.append(
                            {
                                "first_name": author_name,
                                "last_name": author_last_name,
                            }
                        )

                parsed_list.append(
                    {
                        "title": topic.text,
                        "href": trending_news_url,
                        "author": authors_list,
                    }
                )

            else:
                parsed_list.append(
                    {
                        "title": topic.text,
                        "href": trending_news_url,
                        "author": authors_list,
                    }
                )

        all_topics = soup.find(class_="main_content")
        news_within_main = all_topics.find_all(class_="article_header")

        for count, news in enumerate(news_within_main):
            if news.find("em"):
                news.find("em").decompose()
            news_link = news.find("a")["href"]
            if (
                news_link.startswith("http")
                or news_link.startswith("/columns")
                or news_link.startswith("www")
            ):
                parsed_list.append(
                    {
                        "title": news.find("a").text,
                        "href": trending_news_url,
                        "author": authors_list,
                    }
                )
            else:
                sub_page = requests.get((URL + news_link))
                sub_soup = BeautifulSoup(sub_page.content, "html.parser")
                date_and_author = sub_soup.find(class_="post_time")
                list_of_authors_and_time = date_and_author.text.split("—")

                authors_list = []
                authors = list_of_authors_and_time[0].split(",")
                for author in authors:
                    if author:
                        name_parts = author.split()
                        author_name = name_parts[0]
                        author_last_name = name_parts[1] if len(name_parts) > 1 else ""
                        authors_list.append(
                            {
                                "first_name": author_name,
                                "last_name": author_last_name,
                            }
                        )

                parsed_list.append(
                    {
                        "title": news.find("a").text,
                        "href": URL + news_link,
                        "author": authors_list,
                    }
                )

        return parsed_list


class Manager:
    """
    1. Initialize Storage class
    2. Initialize Parser class
    3. Call parser.
    4. serach for existed Authors, create not existed
    5. save news
    """

    def __init__(self) -> None:
        self.parser = Parser()
        self.storage = Storage(dsn)

    def search_for_authors(self, parser_results):
        for news_item in parser_results:
            for author in news_item["author"]:
                first_name = author["first_name"]
                last_name = author["last_name"]
                self.storage.check_if_author_is_in_base(first_name, last_name)

    def main(self):
        parser_results = self.parser.parse_news()
        print(parser_results)
        with self.storage:
            self.storage.check_if_base_is_created()
            self.search_for_authors(parser_results)
            self.storage.save_parsed_data(parser_results)


if __name__ == "__main__":
    manager = Manager()
    manager.main()
