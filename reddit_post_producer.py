import configparser, argparse
import praw
from datetime import datetime
from dateutil import tz
from kafka import KafkaProducer
import json


class MessageProducer:
    broker = ""
    topic = ""
    producer = None

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )

    def send_msg(self, msg):
        print("sending message...")
        try:
            future = self.producer.send(self.topic, msg)
            self.producer.flush()
            future.get(timeout=60)
            print("message sent successfully...")
            return {"status_code": 200, "error": None}
        except Exception as ex:
            return ex


def generate_message_producer_obj(broker, topic):
    return MessageProducer(broker, topic)


def get_credentials(credential_file):
    credentials = configparser.ConfigParser()
    credentials.read(credential_file)

    id = credentials["ACCESS"]["CLIENT_ID"]
    secret = credentials["ACCESS"]["CLIENT_SECRET"]
    agent = credentials["ACCESS"]["USER_AGENT"]

    return id, secret, agent


def get_reddit_instance(client_id, client_secret, user_agent):
    return praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)


def get_reddit_posts(reddit, subreddit_name, message_producer):
    for submission in reddit.subreddit(subreddit_name).new(limit=5):
        data = {
            "submission_id": submission.id,
            "subreddit": subreddit_name,
            "creation_time": str(
                datetime.fromtimestamp(submission.created_utc).astimezone(
                    tz.gettz("IST")
                )
            ),
            "title": submission.title,
            "author": submission.author.name,
            "is_NSFW": submission.over_18,
            "upvotes": submission.score,
            "url": submission.url,
        }
        message_producer.send_msg(data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-creds", "--credentials",  help="path to the file with info to access the service"
    )
    parser.add_argument("-sub", "--subreddit",  help="the subreddit to fetch posts from")
    parser.add_argument(
        "-b",
        "--broker",
        help="server:port of the Kafka broker where messages will be published",
    )
    parser.add_argument("-t", "--topic", help="topic where messages will be published")
    args = parser.parse_args()

    message_producer = generate_message_producer_obj(args.broker, args.topic)
    client_id, client_secret, user_agent = get_credentials(args.credentials)
    reddit_instance = get_reddit_instance(client_id, client_secret, user_agent)
    get_reddit_posts(reddit_instance, args.subreddit, message_producer)