import importer
import querier
import pandas as pd

def get_select_lists(command):
    i = 1
    select = []
    from_ = []
    where = []
    while command[i] != 'from':
        select.append(command[i])
        i += 1
    i += 1
    while command[i] != 'where':
        from_.append(command[i])
        i += 1
    i += 1
    while i < len(command):
        where.append(command[i])
        i += 1
    return (select, from_, where)


def interpret(command, db):
    if command[0] == 'import':
        if command[1] == 'csv':
            path_array = command[2:]
            importer.import_csv(path_array, db)

        if command[1] == 'mysql':
            conn_params = {}    
            conn_params['host'] = input('Digite o host: ')
            conn_params['user'] = input('Digite o usuário: ')
            conn_params['password'] = input('Digite a senha: ')
            conn_params['database'] = input('Digite o nome do banco de dados: ')
            conn_params['port'] = int(input('Digite o número da porta: '))

            # Solicitar as tabelas até que a palavra 'to' seja digitada
            table_array = []
            while True:
                table_name = input('Digite o nome da tabela a ser adicionada (ou "to" para parar): ')
                if table_name.lower() == 'to':
                    break
                table_array.append(table_name)

            importer.import_mysql(conn_params, table_array, db)

    if command[0] == 'select':
        (select, from_tables, where) = get_select_lists(command)
        from_ = []
        for table in from_tables:
            dataframe = pd.read_csv("./dbs/" + db + "/" + table + ".csv", sep = ';')
            from_.append(dataframe)
        querier.query(select, from_, where, db)

    
