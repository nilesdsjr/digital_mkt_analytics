import sqlalchemy 

engine = sqlalchemy.create_engine('postgresql://postgres:Postgres@localhost:15432/postgres')
conn = engine.connect()
conn.connection.connection.set_isolation_level(0)
conn.execute("SELECT pg_terminate_backend(pid) \
FROM pg_stat_activity \
WHERE datname = ' mkt_db';")
conn.execute("DROP DATABASE IF EXISTS mkt_db;")
conn.execute("CREATE DATABASE mkt_db \
    WITH \
    OWNER = postgres \
    ENCODING = 'UTF8' \
    CONNECTION LIMIT = -1;")

conn.connection.connection.set_isolation_level(1)
engine.dispose()