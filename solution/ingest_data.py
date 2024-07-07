#!/usr/bin/env python3
import mysql.connector
import pandas as pd
from mysql.connector import Error
from sqlalchemy import create_engine


def create_sqlalchemy_engine():
    return create_engine('mysql+mysqlconnector://root:password@db:3306/mydb')

def create_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='db',
            database='mydb',
            user='root',
            password='password'
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL database: {e}")
        return None

def insert_data_sqlalchemy(engine, table, data):
    data.to_sql(table, engine, if_exists='append', index=False)

def insert_data_mysql(connection, table, data):
    cursor = connection.cursor()
    placeholders = ', '.join(['%s'] * len(data.columns))
    columns = ', '.join(data.columns)
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    
    for _, row in data.iterrows():
        cursor.execute(sql, tuple(row))
    
    connection.commit()
    cursor.close()

def main():
    try:
        engine = create_sqlalchemy_engine()
        connection = create_mysql_connection()

        if not connection:
            return

        # Load places data
        places_df = pd.read_csv('/data/places.csv')
        
        try:
            insert_data_sqlalchemy(engine, 'places', places_df)
        except Exception as e:
            print(f"SQLAlchemy insertion failed: {e}. Trying mysql-connector.")
            insert_data_mysql(connection, 'places', places_df)
        
        print("Places data inserted successfully.")

        # Load people data
        people_df = pd.read_csv('/data/people.csv')
        print("Columns in people.csv:", people_df.columns)

        # # Get place IDs
        # place_id_map = pd.read_sql('SELECT id, city FROM places', engine).set_index('city')['id'].to_dict()

        # # Map place of birth to place ID
        # people_df['place_of_birth'] = people_df['place_of_birth'].map(place_id_map)
        # people_df = people_df.drop(columns=['place_of_birth'])

        try:
            insert_data_sqlalchemy(engine, 'people', people_df)
        except Exception as e:
            print(f"SQLAlchemy insertion failed: {e}. Trying mysql-connector.")
            insert_data_mysql(connection, 'people', people_df)
        
        print("People data inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()