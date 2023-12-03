import os
import csv
import psycopg2
import mysql.connector
import sys

class DataBase:
    def __init__(self):
        self.banco_dados_interno = {}

    def importar_csv_usuario(self):
        print('Digite o caminho do CSV para importar:')
        input_path = input('>> ')

        if not os.path.isfile(input_path):
            print("CSV não encontrado!")
            return

        print('Digite o caminho de saída para o CSV:')
        output_path = input('>> ')

        if os.path.isfile(output_path):
            confirm = input("O arquivo já existe, deseja sobrescrever? (y/n): ")
            if confirm.lower() != 'y':
                print("Operação cancelada!")
                return

        open(output_path, 'w').close()

        try:
            with open(input_path, 'r') as input_file, open(output_path, 'w', newline='') as output_file:
                csv_reader = csv.reader(input_file)
                csv_writer = csv.writer(output_file)

                for line in csv_reader:
                    csv_writer.writerow(line)

            print("Importação concluída!")
        except Exception as e:
            print("Erro:", str(e))

    def importar_mysql(self):
        host_glob = os.environ.get('HOST_MYSQL')
        user_glob = os.environ.get('USER_MYSQL')
        password_glob = os.environ.get('PASSWORD_MYSQL')
        port_glob = os.environ.get('PORT_MYSQL')
        database_glob = None

        def mysqlconnect():
            conn_params = {
                'host': host_glob,
                'user': user_glob,
                'password': password_glob,
                'database': database_glob,
                'port': port_glob
            }

            try:
                db_connection = mysql.connector.connect(**conn_params)
            except:
                print("Erro: Esquema não encontrado")
                return False

            print('Conectado ao servidor!')
            return db_connection

        def mysql_check_table(table, cursor):
            try:
                query = ('select * from {}').format(table)
                cursor.execute(query)
                return True
            except:
                return False

        def show_tables(cursor):
            print("Tabelas em {}:".format(database_glob))
            cursor.execute("show tables;")
            for row in cursor:
                for key in row:
                    print('* ' + row[key].strip("'"))

        def show_database():
            conn = mysql.connector.connect(user=user_glob, password=password_glob,
                                           host=host_glob, buffered=True)
            cursor = conn.cursor()
            databases = ("show databases;")
            cursor.execute(databases)
            print("Esquemas no servidor MySQL:")
            for (databases) in cursor:
                print('* ' + databases[0])

        def mysqlimport():
            nonlocal database_glob

            show_database()
            conn = None
            while not conn:
                print("Selecione o esquema:")
                database_glob = input('>> ')
                conn = mysqlconnect()

            cursor = conn.cursor(dictionary=True, buffered=True)

            show_tables(cursor)
            print('Digite a tabela para importar : ')
            table = input('>> ')

            while True:
                if mysql_check_table(table, cursor):
                    break
                else:
                    print("Erro: Tabela não existe no servidor")
                    print('Digite a tabela para importar : ')
                    table = input('>> ')

            if not engine.check_existing_schema(schema=database_glob):

                create_folder = None
                while not (create_folder == 'y' or create_folder == 'n'):
                    print('Esquema não encontrado localmente, deseja criar? (y/n)')
                    create_folder = input('>> ')

                if create_folder == 'y':
                    engine.create_schema(schema=database_glob)
                else:
                    return True

            if engine.check_existing_table(table, schema=database_glob):
                overwrite = None
                while not (overwrite == 'y' or overwrite == 'n'):
                    print('A tabela já existe, deseja sobrescrever? (y/n)')
                    overwrite = input('>> ')
                if overwrite == 'y':
                    headers = cursor.column_names
                    engine.write_csv(table, cursor, headers, schema=database_glob)
                elif overwrite == 'n':
                    return True
            else:
                # cria um novo arquivo com o nome da tabela
                headers = cursor.column_names
                engine.write_csv(table, cursor, headers, schema=database_glob)

            cursor.close()
            conn.close()

            return True

        def data_import():
            option = None
            while not (option == "mysql" or option == "postgres" or option == "csv"):
                print("Select csv or server (mysql or postgres)")
                option = input(">> ")
            if option == "mysql":
                mysqlimport()
            elif option == "postgres":
                postgresimport()
            elif option == "csv":
                self.importar_csv_usuario()
            return

        def postgresconect():
            conn_params = {
                'database': database_glob,
                'user': user_glob,
                'host': host_glob,
                'password': password_glob,
                'port': port_glob,
            }

            try:
                db_connection = psycopg2.connect(**conn_params)
            except:
                print("Erro: Esquema não encontrado")
                return 0

            print('Conectado ao servidor!')
            return db_connection

        def postgres_check_table(table, cursor):
            try:
                query = ('select * from {}').format(table)
                cursor.execute(query)
                return 1
            except:
                return 0

        def show_tables(cursor):
            print("Tabelas em {}:".format(database_glob))
            cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
            for row in cursor:
                print('* ' + row[1])

        def show_database():
            db_connection = psycopg2.connect(user=user_glob, password=password_glob,
                                             host=host_glob, port=port_glob)
            cursor = db_connection.cursor()
            databases = "SELECT datname FROM pg_database WHERE datistemplate = false;"
            cursor.execute(databases)
            print("Esquemas no servidor POSTGRES:")
            for databases in cursor:
                print("* " + databases[0].strip("'"))

        def postgresimport():
            nonlocal database_glob

            show_database()
            conn = None
            while not conn:
                print("Selecione o esquema:")
                database_glob = input('>> ')
                conn = self.postgresconect()

            cursor = conn.cursor()

            show_tables(cursor)
            print('Digite a tabela para importar: ')
            table = input('>> ')

            if self.postgres_check_table(table, cursor):

                if not engine.check_existing_schema(schema=database_glob):

                    create_folder = None
                    while not (create_folder == 'y' or create_folder == 'n'):
                        print('Esquema não encontrado localmente, deseja criar? (y/n)')
                        create_folder = input('>> ')

                    if create_folder == 'y':
                        engine.create_schema(schema=database_glob)
                    else:
                        return 0

                if engine.check_existing_table(table, schema=database_glob):
                    overwrite = None
                    while not (overwrite == 'y' or overwrite == 'n'):
                        print('A tabela já existe, deseja sobrescrever? (y/n)')
                        overwrite = input('>> ')
                    if overwrite == 'y':
                        headers = [desc[0] for desc in cursor.description]
                        cursorDict = [dict(zip(headers, row)) for row in cursor.fetchall()]
                        engine.write_csv(table, cursorDict, headers, schema=database_glob)
                    elif overwrite == 'n':
                        return 0
                else:
                    # cria um novo arquivo com o nome da tabela
                    headers = [desc[0] for desc in cursor.description]
                    cursorDict = [dict(zip(headers, row)) for row in cursor.fetchall()]
                    engine.write_csv(table, cursorDict, headers, schema=database_glob)

            else:
                print("Erro: Tabela não existe no servidor")
                return 0

            for row in cursor:
                print(row)

            cursor.close()
            conn.close()

if __name__ == "__main__":
    db = DataBase()
    db.importar_mysql()
