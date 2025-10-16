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
        if dataBase.is_connected():
            print("Connected to MySQL database successfully")
            
        cursor = dataBase.cursor()
        return dataBase, cursor
        
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

client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='')

query_stock = """select 
ma.name as activity_name,
s.qty,
s.createdAt as created_at,
s.updatedAt as update_at,
ehmm.master_material_id,
ehmm.entity_id
from stocks s 
inner join entity_has_master_materials ehmm on s.entity_has_material_id = ehmm.id
inner join master_activities ma on s.activity_id = ma.id"""

conn, cursor = connection_db()
stocks = get_data_from_table(cursor, query_stock)
materials = get_data_from_table(cursor, "select id, name as material_name, unit_of_distribution from master_materials")
entities = get_data_from_table(cursor, "select id, name as entities_name, address, province_id, regency_id, lat, lng as lang from entities")
provinces = get_data_from_table(cursor, "select id as province_id, name as name_provinces from provinces")
regencies = get_data_from_table(cursor, "select id as regency_id, name as name_regency from regencies")
print("selesai mengambil semua data")

merge_data = (
    stocks
    .merge(materials, left_on="master_material_id", right_on="id", how="inner")
    .merge(entities, left_on="entity_id", right_on="id", how="inner")
    .merge(provinces, left_on="province_id", right_on="province_id", how="inner")
    .merge(regencies, left_on="regency_id", right_on="regency_id", how="inner")
    .drop(columns=["master_material_id", "entity_id", "id_x", "id_y"])
)

print("selesai menggambungkan semua data")
print(merge_data.head())

merge_data.to_excel("hasil gabungan.xlsx")
cursor.close()
conn.close()

