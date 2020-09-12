from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:Postgres@localhost:15432/postgres')
conn = engine.connect()
conn.connection.connection.set_isolation_level(0)
conn.execute("SELECT pg_terminate_backend(pid) \
FROM pg_stat_activity \
WHERE datname = 'dteng_quero_db';")
conn.execute("DROP DATABASE IF EXISTS dteng_quero_db;")
conn.execute("CREATE DATABASE dteng_quero_db \
    WITH \
    OWNER = postgres \
    ENCODING = 'UTF8' \
    CONNECTION LIMIT = -1;")

conn.connection.connection.set_isolation_level(1)