import psycopg2
from common import config



# 连接数据库
conn = psycopg2.connect(config.DB_URL)

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