import sqlite3
conn = sqlite3.connect("C:/Users/saite/Downloads/nse_data.db")
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM dhan_nse")
count = cursor.fetchone()[0]
print(f"I found {count} toys in SQLite!")
cursor.execute("SELECT * FROM dhan_nse LIMIT 5")
for toy in cursor.fetchall():
    print(toy)
conn.close()