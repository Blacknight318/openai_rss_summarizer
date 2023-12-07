import json
import openai
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import feedparser
from newspaper import Article
import sqlite3
from datetime import datetime
import time


def load_config():
    with open('config.json', 'r') as file:
        return json.load(file)


def create_database():
    conn = sqlite3.connect('articles.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS articles
                 (link TEXT PRIMARY KEY, title TEXT, summary TEXT)''')
    conn.commit()
    conn.close()


def is_article_summarized(link):
    conn = sqlite3.connect('articles.db')
    c = conn.cursor()
    c.execute("SELECT * FROM articles WHERE link = ?", (link,))
    result = c.fetchone()
    conn.close()
    return result


def save_summary(link, title, summary):
    conn = sqlite3.connect('articles.db')
    c = conn.cursor()
    c.execute("INSERT INTO articles (link, title, summary) VALUES (?, ?, ?)", (link, title, summary))
    conn.commit()
    conn.close()


def create_thread(ass_id, prompt):
    thread = openai.beta.threads.create()
    my_thread_id = thread.id

    openai.beta.threads.messages.create(
        thread_id=my_thread_id,
        role="user",
        content=prompt
    )

    run = openai.beta.threads.runs.create(
        thread_id=my_thread_id,
        assistant_id=ass_id,
    )

    return run.id, my_thread_id


def check_status(run_id, thread_id):
    run = openai.beta.threads.runs.retrieve(
        thread_id=thread_id,
        run_id=run_id,
    )
    return run.status


def send_message_to_slack(title, link, summary):
    try:
        message = f"New Article: *<{link}|{title}>*\nSummary: {summary}"
        client.chat_postMessage(channel='#news', text=message)
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")


def fetch_articles_from_rss(rss_url):
    feed = feedparser.parse(rss_url)
    for entry in feed.entries:
        if not is_article_summarized(entry.link):
            article = Article(entry.link)
            article.download()
            article.parse()

            prompt = f"Please summarize this article:\n\nTitle: {entry.title}\n\n{article.text}"
            run_id, thread_id = create_thread(assistant_id, prompt)

            status = check_status(run_id, thread_id)
            while status != "completed":
                status = check_status(run_id, thread_id)
                time.sleep(2)

            response = openai.beta.threads.messages.list(thread_id=thread_id)
            if response.data:
                summary = response.data[0].content[0].text.value
                # Send the article details to Slack
                send_message_to_slack(entry.title, entry.link, summary)
                save_summary(entry.link, entry.title, summary)

            time.sleep(20)


def main():
    create_database()
    while True:
        now = datetime.now()
        print(f'Punch in at {now}')
        for rss_url in config['rss_urls']:
            fetch_articles_from_rss(rss_url)
        now = datetime.now()
        print(f'Punch out at {now}')
        time.sleep(900)


if __name__ == "__main__":
    config = load_config()

    # Set the API keys from the configuration
    openai.api_key = config['openai_key']
    assistant_id = config['assistant_id']

    client = WebClient(token=config['slack_token'])

    main()
