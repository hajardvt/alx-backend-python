#!/usr/bin/python3
import os
import csv
import mysql.connector
from mysql.connector import errorcode


DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_NAME = "ALX_prodev"


def connect_db():
    """connects to the mysql database server (no DB selected)"""
    try:
        return mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS
        )
    except mysql.connector.Error as e:
        print(f"DB connection error: {e}")
        return None


def create_database(connection):
    """creates the database ALX_prodev if it does not exist"""
    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    finally:
        cursor.close()


def connect_to_prodev():
    """connects to the ALX_prodev database in MYSQL"""
    try:
        return mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
        )
    except mysql.connector.Error as e:
        if e.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist. Did you run create_database()?")
        else:
            print(f"DB connection error: {e}")
        return None


def create_table(connection):
    """creates a table user_data if it does not exist with the required fields"""
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_data (
              user_id CHAR(36) PRIMARY KEY,
              name VARCHAR(255) NOT NULL,
              email VARCHAR(255) NOT NULL,
              age DECIMAL(5,0) NOT NULL,
              INDEX (user_id)
            )
            """
        )
        connection.commit()
        print("Table user_data created successfully")
    finally:
        cursor.close()


def insert_data(connection, csv_path):
    """inserts data in the database if it does not exist"""
    cursor = connection.cursor()
    try:
        with open(csv_path, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = [
                (r["user_id"], r["name"], r["email"], r["age"])
                for r in reader
            ]
        # Insert if not exists by primary key
        cursor.executemany(
            """
            INSERT IGNORE INTO user_data (user_id, name, email, age)
            VALUES (%s, %s, %s, %s)
            """,
            rows
        )
        connection.commit()
    finally:
        cursor.close()
