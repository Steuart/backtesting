import psycopg2
from psycopg2 import sql

# 连接数据库
conn = psycopg2.connect(
    dbname="trader",
    user="root",
    password="hy123!!!",
    host="localhost",
    port="5432"
)

# 创建游标
cur = conn.cursor()

# 执行查询
cur.execute("SELECT version();")
db_version = cur.fetchone()
print(f"PostgreSQL version: {db_version}")

# 查询数据
cur.execute("SELECT * FROM fund_market where symbol='159117.SZ'")
rows = cur.fetchall()
for row in rows:
    print(row)

# 关闭连接
cur.close()
conn.close()