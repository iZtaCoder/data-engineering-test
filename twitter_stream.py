import json

from tweepy import StreamingClient, StreamRule, Tweet
from config import BEARER_TOKEN
from save_data import SaveTweets


class StreamTweets(StreamingClient):
    def __init__(self, user_name: str, limit: int = 100, **kwargs):
        super().__init__(**kwargs)
        self.limit = limit
        self.user_name = user_name
        self.tweets = []

    def on_connect(self):
        print("Connected")

    def on_tweet(self, tweet):
        if len(self.tweets) == self.limit:
            print("I'm full...")
            self.disconnect()

    def on_data(self, raw_data):
        tweet_dict = json.loads(raw_data)
        self.tweets.append(tweet_dict["data"])
        print(f'{tweet_dict["data"]}'.encode("unicode-escape"))
        print("__" * 55)
        return super().on_data(raw_data)

    def on_disconnect(self):
        if len(self.tweets) > 0:
            st = SaveTweets(self.tweets)
            st.save_in_parquet_file(f"{self.user_name}_tweets")
            st.save_in_db(f"{self.user_name}_tweets")
        return super().on_disconnect()

