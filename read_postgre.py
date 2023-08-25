import pandas as pd
from sqlalchemy import create_engine

def ler_tabela_postgres():
    try:
        # Solicitar informações de conexão ao usuário
        host = input("Digite o host do PostgreSQL: ")
        port = input("Digite a porta do PostgreSQL: ")
        database = input("Digite o nome do banco de dados: ")
        user = input("Digite o nome de usuário: ")
        password = input("Digite a senha: ")
        schema = input("Digite o nome do esquema: ")
        table = input("Digite o nome da tabela: ")

        # Criar a string de conexão usando sqlalchemy
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)

        # Ler a tabela e criar um DataFrame Pandas
        query = f"SELECT * FROM {schema}.{table}"
        df = pd.read_sql(query, engine)

        return df
    except Exception as e:
        print("Erro ao ler tabela:", e)

def main():
    df = ler_tabela_postgres()

    if df is not None:
        print("Dados lidos da tabela:")
        print(df)

if __name__ == "__main__":
    main()
