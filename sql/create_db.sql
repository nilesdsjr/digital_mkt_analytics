from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:Postgres@localhost:15432/postgres')
conn = engine.connect()
conn.connection.connection.set_isolation_level(0)

conn.execute("CREATE DATABASE dteng_quero_db \
    WITH \
    OWNER = postgres \
    ENCODING = 'UTF8' \
    CONNECTION LIMIT = -1;")
conn.connection.connection.set_isolation_level(1)