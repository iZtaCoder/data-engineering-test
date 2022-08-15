import pandas as pd
import fastparquet
import tweepy
import logging

from config import BEARER_TOKEN
from db_engine import DBEngine


def get_twitter_client(bearer_token):
    # initialize the twitter API
    return tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)


def get_tweets(user_name, limit, fields):
    try:
        client = get_twitter_client(BEARER_TOKEN)

        query = f"from:{user_name} -is:retweet"  # query for fetch the user tweets except retweets

        tweets = tweepy.Paginator(
            client.search_recent_tweets,
            query=query,
            tweet_fields=fields,
            max_results=100,
        ).flatten(limit=limit)
        if not tweets:
            return None

        return [tweet.data for tweet in tweets]

    except tweepy.TweepyException as err:
        logging.error(err.__str__)
        return None


def save_tweets(tweets: list):
    try:
        # if the user have tweets then create a dataframe
        df = pd.DataFrame.from_records(tweets)

        # create the parquet file from dataframe
        df.to_parquet(f"{user_name}_tweets.parquet", engine="fastparquet")

        # create a DB instance for get the engine
        db = DBEngine()

        # translate the colum names for postgres table
        df.columns = [c.lower() for c in df.columns]

        # load the dataframe with tweets to a postgres table
        df.to_sql(f"{user_name}", db.get_engine())

        return True
    except Exception as err:
        print(err.__str__)
        return False


if __name__ == "__main__":
    client = get_twitter_client(BEARER_TOKEN)

    user_name = "BBCWorld"  # name of the user we want to search for
    limit = 150  # number of tweets
    query = f"from:{user_name} -is:retweet"  # query for fetch the user tweets except retweets
    fields = [
        "created_at",
        "author_id",
        "source",
        "lang",
    ]  # data that the tweet must to have

    # receive the last tweets
    tweets = get_tweets(user_name=user_name, limit=limit, fields=fields)
    if tweets:
        # if there are tweets then save them in a parquet file and SQL table
        save_tweets(tweets)
