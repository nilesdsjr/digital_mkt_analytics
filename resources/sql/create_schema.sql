import sqlalchemy 

schema_name = ['stage','mkt_digital']
engine = sqlalchemy.create_engine('postgresql://postgres:Postgres@localhost:15432/mkt_db')
for item in schema_name:
    if not engine.dialect.has_schema(engine, item):
        engine.execute(sqlalchemy.schema.CreateSchema(item))
engine.dispose()