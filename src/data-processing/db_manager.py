from dotenv import load_dotenv
import os
import sqlalchemy

load_dotenv()

def get_sql_engine() -> sqlalchemy.engine.base.Engine:
  db_connection_string = os.getenv('DB_CONNECTION_STRING')
  db_engine = sqlalchemy.create_engine(db_connection_string)
  return db_engine
