import mysql.connector # realizar ligacao com mysql
import os # manipulacao de arquivos e diretorios

# Le uma lista de arquivos .csv, e importa eles pro banco de dados
# db: diretório banco de dados
def import_csv(path_array, db):
    for path in path_array:
        try:
            import_file = open(path, "r")
        except:
            print("Error opening import file: " + path)
            continue
        db_table = "./dbs/" + db + "/" + path.split("/")[-1]
        try:
            table_file = open(db_table, "a+")  # Open the file in append mode
        except: 
            print("Error creating table: " + path)
            continue

        table_file.write("\n"+import_file.read())
        import_file.close()
        table_file.close()

# Le uma lista de tabelas mysql, e importa elas pro banco de dados
def import_mysql(conn_params, table_array, db):
    # Conecta ao banco de dados
    def mysqlconnect(conn_params):
        try:
            db_connection = mysql.connector.connect(**conn_params)
            return True
        except:
            print('Erro ao conectar ao banco de dados!')
            return False

    def import_table(table, cursor, db):
        # Cria o arquivo da tabela
        try:
            table_file = open("./dbs/" + table + ".csv", "a+")  # Open the file in append mode
        except:
            print("Error creating table: " + table)
            return False
        
        # Escreve no arquivo da tabela
        cursor.execute("select * from " + table + ";")
        for row in cursor:
            for key in row:
                table_file.write(row[key] + ",")
            table_file.write("\n")
        table_file.close()
        return True
    

    