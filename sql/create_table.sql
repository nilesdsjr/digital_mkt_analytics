from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:Postgres@localhost:15432/dteng_quero_db')
connection = engine.connect()
connection.execute('DROP TABLE IF EXISTS caged CASCADE;')
connection.execute('CREATE TABLE caged (\
categoria INTEGER,\
cbo2002_ocupacao INTEGER,\
competencia INTEGER,\
fonte INTEGER,\
grau_de_instrucao INTEGER,\
horas_contratuais INTEGER,\
id INTEGER,\
idade INTEGER,\
ind_trab_intermitente INTEGER,\
ind_trab_parcial INTEGER,\
indicador_aprendiz INTEGER,\
municipio INTEGER,\
raca_cor INTEGER,\
regiao INTEGER,\
salario INTEGER,\
saldo_movimentacao INTEGER,\
secao INTEGER,\
sexo INTEGER,\
subclasse INTEGER,\
tam_estab_jan INTEGER,\
tipo_de_deficiencia INTEGER,\
tipo_empregador INTEGER,\
tipo_estabelecimento INTEGER,\
tipo_movimentacao INTEGER,\
uf INTEGER) ;')
connection.execute('CREATE INDEX caged_id ON caged (id);')
connection.execute('DROP TABLE IF EXISTS caged CASCADE;')
connection.execute('CREATE TABLE caged_no_success ( \
categoria INTEGER,\
cbo2002_ocupacao INTEGER,\
competencia INTEGER,\
fonte INTEGER,\
grau_de_instrucao INTEGER,\
horas_contratuais INTEGER,\
id INTEGER,\
idade INTEGER,\
ind_trab_intermitente INTEGER,\
ind_trab_parcial INTEGER,\
indicador_aprendiz INTEGER,\
municipio INTEGER,\
raca_cor INTEGER,\
regiao INTEGER,\
salario INTEGER,\
saldo_movimentacao INTEGER,\
secao INTEGER,\
sexo INTEGER,\
subclasse INTEGER,\
tam_estab_jan INTEGER,\
tipo_de_deficiencia INTEGER,\
tipo_empregador INTEGER,\
tipo_estabelecimento INTEGER,\
tipo_movimentacao INTEGER,\
uf INTEGER) ;')
connection.execute('CREATE INDEX ncaged_id ON caged_no_success (id);')

