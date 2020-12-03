import datetime
from dotenv import load_dotenv
import os
import sqlalchemy
import pandas as pd

load_dotenv()

def get_sql_engine() -> sqlalchemy.engine.base.Engine:
  db_connection_string = os.getenv('DB_CONNECTION_STRING')
  db_engine = sqlalchemy.create_engine(db_connection_string)
  return db_engine

def query_to_dataframe(sql: sqlalchemy.sql.text, engine = None) -> pd.DataFrame:
  if engine is None:
    engine = get_sql_engine()
  results = engine.execute(sql)
  df = pd.DataFrame(results.fetchall())
  df.columns = results.keys()
  return df

def get_total_monthly_users(engine = None) -> pd.DataFrame:
  sql = sqlalchemy.sql.text("""
                              SELECT
                                year,
                                month,
                                COUNT(*)
                              FROM (
                                SELECT DISTINCT 
                                  user_id,
                                  EXTRACT(year FROM time)::int AS year, 
                                  EXTRACT(month FROM time)::int AS month
                                FROM action_events 
                              ) monthly_active_users
                              GROUP BY year, month
                              ORDER BY year, month ASC;
                            """)
  total_monthly_users = query_to_dataframe(sql, engine)
  total_monthly_users['date'] = total_monthly_users.apply(lambda row: datetime.date(row['year'], row['month'], 1), axis=1)
  return total_monthly_users

def get_total_new_monthly_users(engine = None) -> pd.DataFrame:
  sql = sqlalchemy.sql.text("""
                              SELECT year, month, COUNT(*)
                              FROM (
                                SELECT
                                user_id,
                                min(year) AS year,
                                min(month) AS month
                                FROM (
                                  SELECT DISTINCT 
                                  user_id,
                                  EXTRACT(year FROM time)::int AS year, 
                                  EXTRACT(month FROM time)::int AS month
                                  FROM action_events 
                                ) monthly_active_users
                                GROUP BY user_id
                              ) monthly_new_users
                                GROUP BY year, month
                                ORDER BY year, month ASC;
                            """)
  total_new_monthly_users = query_to_dataframe(sql, engine)
  total_new_monthly_users['date'] = total_new_monthly_users.apply(lambda row: datetime.date(row['year'], row['month'], 1), axis=1)
  return total_new_monthly_users

def get_total_returning_monthly_users(engine = None) -> pd.DataFrame:
  sql = sqlalchemy.sql.text("""
                              SELECT 
                                year,
                                month,
                                COUNT(*)
                              FROM 
                              (
                                (
                                  SELECT DISTINCT 
                                    user_id,
                                    EXTRACT(year FROM time)::int AS year, 
                                    EXTRACT(month FROM time)::int AS month
                                  FROM action_events 
                                )
                                EXCEPT
                                (SELECT
                                  user_id,
                                  min(year) AS year,
                                  min(month) AS month
                                  FROM (
                                    SELECT DISTINCT 
                                    user_id,
                                    EXTRACT(year FROM time)::int AS year, 
                                    EXTRACT(month FROM time)::int AS month
                                    FROM action_events 
                                  ) monthly_active_users
                                  GROUP BY user_id
                                )
                              ) returning_users_by_month
                              GROUP BY year, month
                              ORDER BY year, month ASC;
                            """)
  total_returning_monthly_users = query_to_dataframe(sql, engine)
  total_returning_monthly_users['date'] = total_returning_monthly_users.apply(lambda row: datetime.date(row['year'], row['month'], 1), axis=1)
  return total_returning_monthly_users



def get_total_new_monthly_users_by_country(engine = None) -> pd.DataFrame:
  sql = sqlalchemy.sql.text("""
                              SELECT year, month, country, COUNT(*)
                              FROM (
                                SELECT
                                user_id,
                                country,
                                min(year) AS year,
                                min(month) AS month
                                FROM (
                                  SELECT DISTINCT 
                                  u.user_id,
                                  u.country,
                                  EXTRACT(year FROM time)::int AS year, 
                                  EXTRACT(month FROM time)::int AS month
                                  FROM action_events evt
                                  JOIN users u
                                  ON evt.user_id = u.user_id
                                ) monthly_active_users
                                GROUP BY user_id, country
                              ) monthly_new_users
                                GROUP BY year, month, country
                                ORDER BY year, month ASC;
                            """)
  total_new_monthly_users_by_country = query_to_dataframe(sql, engine)
  return total_new_monthly_users_by_country

def get_total_monthly_users_by_country(engine = None) -> pd.DataFrame:
  sql = sqlalchemy.sql.text("""
                              SELECT
                                year,
                                month,
                                country,
                                COUNT(*)
                              FROM (
                                SELECT DISTINCT 
                                  u.user_id,
                                  country,
                                  EXTRACT(year FROM time)::int AS year, 
                                  EXTRACT(month FROM time)::int AS month
                                  FROM action_events evt
                                  JOIN users u
                                  ON evt.user_id = u.user_id
                                ) monthly_active_users
                                GROUP BY year, month, country
                                ORDER BY year, month ASC;
                            """)
  total_monthly_users_by_country = query_to_dataframe(sql, engine)
  return total_monthly_users_by_country
  