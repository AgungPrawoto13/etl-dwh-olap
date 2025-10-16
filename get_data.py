import os
import pandas as pd
import mysql.connector
import clickhouse_connect

from dotenv import load_dotenv

load_dotenv()

def connection_db():
    try:
        dataBase = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            user=os.getenv("DB_USER"),
            passwd=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        
        client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')
        version = client.query('SELECT version()').result_rows
        print("Koneksi berhasil! Versi ClickHouse:", version)

        if dataBase.is_connected():
            print("Connected to MySQL database successfully")
            
        cursor = dataBase.cursor()
        return client, dataBase, cursor
        
    except mysql.connector.Error as e:
        print("waduh error nih bang messi, kacau", e)

def get_data_from_table(cursor, query):
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        column_names = [i[0] for i in cursor.description]
        df = pd.DataFrame(result, columns=column_names)
        return df
    
    except mysql.connector.errors.ProgrammingError as e:
        print("gak bisa ambil data nih bang messi", e)

def create_table(client):
    print("create table")
    client.command("""
    CREATE DATABASE IF NOT EXISTS datamart_stok
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS datamart_stok.dim_material (
        material_id UInt32,
        material_name String,
        unit_of_distribution String
    ) ENGINE = MergeTree() ORDER BY material_id
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS datamart_stok.dim_entity (
        entity_id UInt32,
        entities_name String,
        address String,
        lat Float64,
        lang Float64
    ) ENGINE = MergeTree() ORDER BY entity_id
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS datamart_stok.dim_province (
        province_id UInt32,
        name_provinces String
    ) ENGINE = MergeTree() ORDER BY province_id
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS datamart_stok.dim_regency (
        regency_id UInt32,
        name_regency String
    ) ENGINE = MergeTree() ORDER BY regency_id
    """)

    client.command("""
    CREATE TABLE IF NOT EXISTS datamart_stok.fact_stocks (
        fact_id UInt32,
        material_id UInt32,
        entity_id UInt32,
        qty Float32,
        created_at DateTime
    ) ENGINE = MergeTree() ORDER BY fact_id
    """)

def insert_data_olap(client, df):
    client.insert_df("datamart_stok.dim_material", df[["material_id","material_name","unit_of_distribution"]])
    client.insert_df("datamart_stok.dim_entity", df[["entity_id","entities_name","address","lat","lang"]])
    client.insert_df("datamart_stok.dim_province", df[["province_id","name_provinces"]])
    client.insert_df("datamart_stok.dim_regency", df[["regency_id","name_regency"]])
    client.insert_df("datamart_stok.fact_stocks ", df[["stock_id","material_id","entity_id","qty","created_at"]])


query_stock = """select 
s.id as stock_id,
ma.name as activity_name,
s.qty,
s.createdAt as created_at,
s.updatedAt as update_at,
ehmm.master_material_id,
ehmm.entity_id
from stocks s 
inner join entity_has_master_materials ehmm on s.entity_has_material_id = ehmm.id
inner join master_activities ma on s.activity_id = ma.id"""

client, conn, cursor = connection_db()
create_table(client)
stocks = get_data_from_table(cursor, query_stock)
materials = get_data_from_table(cursor, "select id as material_id, name as material_name, unit_of_distribution from master_materials")
entities = get_data_from_table(cursor, "select id as entities_id, name as entities_name, address, province_id, regency_id, lat, lng as lang from entities")
provinces = get_data_from_table(cursor, "select id as province_id, name as name_provinces from provinces")
regencies = get_data_from_table(cursor, "select id as regency_id, name as name_regency from regencies")
print("selesai mengambil semua data")

merge_data = (
    stocks
    .merge(materials, left_on="master_material_id", right_on="material_id", how="inner")
    .merge(entities, left_on="entity_id", right_on="entities_id", how="inner")
    .merge(provinces, left_on="province_id", right_on="province_id", how="inner")
    .merge(regencies, left_on="regency_id", right_on="regency_id", how="inner")
    # .drop(columns=["master_material_id", "entity_id", "id_x", "id_y"])
)

print("selesai menggambungkan semua data")
print(merge_data.head())

print("insert data to olap")
insert_data_olap(client, merge_data)

merge_data.to_excel("hasil gabungan.xlsx")
cursor.close()
conn.close()

