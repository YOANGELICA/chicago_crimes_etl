import psycopg2 as psy

try:
    conn = psy.connect(
        host= "192.168.100.8",
        user="postgres",
        password = "roj4sxil3na",
        database = "postgres"
    )
    print('Conexión exitosa!')
except psy.Error as e:
        conn = None
        print('No se puede conectar:', e)


cursor = conn.cursor()
cursor.execute("SELECT version()")
row=cursor.fetchone()
print(row)