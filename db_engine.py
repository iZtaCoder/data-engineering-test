from sqlalchemy import create_engine

class DBEngine:

    def __init__(self) -> None:
        self.db_user = "postgres"
        self.db_pwd = "root"
        self.db_host = "localhost"
        self.db_port = "5432"
        self.db_name = "twitter"
    
    def get_engine(self):
        return create_engine(f'postgresql://{self.db_user}:{self.db_pwd}@{self.db_host}:{self.db_port}/{self.db_name}')
