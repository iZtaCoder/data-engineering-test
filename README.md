
# fetch tweets

Project for get the recent tweets from an active Twitter user using the Twitter API v2.
After the fetch, the script saves the data in a parquet file and a SQL table in PostgreSQL.


### Installation

Install the libraries found in the `requirements.txt` file.


### Usage

Allocate your Bearer Token for Twitter API in the file config.
Put your database configuration in `db_engine.py` otherwise it will take a default one.

For run the main script:

`python get_tweets.py`

You can set a user name with the var `user_name` and a limit of records with `limit`


### Maintainers

[@iZtaCoder](https://github.com/iZtaCoder).

 ### License
[MIT](https://choosealicense.com/licenses/mit/)
