import fastparquet
import pandas as pd

from db_engine import DBEngine


class SaveTweets:

    def __init__(self, tweets: list) -> None:
        self.tweets = tweets

        self.dataframe = None
        if self.tweets:
            self.dataframe = pd.DataFrame.from_records([tweet for tweet in self.tweets])
    
    def save_in_parquet_file(self, file_name: str) -> None:
        if not self.dataframe.empty:
            self.dataframe.to_parquet(f"{file_name}.parquet", engine="fastparquet")
    
    def save_in_db(self, table_name: str) -> None:
        if not self.dataframe.empty:
            # create a DB instance for get the engine
            db = DBEngine()

            # translate the colum names for postgres table
            self.dataframe.columns = [column.lower() for column in self.dataframe.columns]

            # load the dataframe with tweets to a postgres table
            self.dataframe.to_sql(f"{table_name}", db.get_engine())


