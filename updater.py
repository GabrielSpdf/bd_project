import pandas as pd

def insert_into(db, table, values_array):
    if len(values_array) == 0:
        print('Erro: Nenhum valor foi selecionado')
        return False

    try:
        file_path = f'./dbs/{db}/{table}.csv'
        
        # Lê o arquivo CSV em um DataFrame
        dataframe = pd.read_csv(file_path, sep=';')

        # Cria um DataFrame a partir da lista de valores
        new_row = pd.DataFrame([values_array + [None] * (len(dataframe.columns) - len(values_array))], columns=dataframe.columns)

        # Adiciona o novo DataFrame ao final do DataFrame original
        dataframe = pd.concat([dataframe, new_row], ignore_index=True)

        # Salva o DataFrame de volta no arquivo CSV
        dataframe.to_csv(file_path, index=False, sep=';')

        return True

    except FileNotFoundError:
        print('Erro: Tabela não encontrada')
        return False

def delete_from(db, table, condition_column, condition_value):
    try:
        file_path = f'./dbs/{db}/{table}.csv'

        # Lê o arquivo CSV em um DataFrame sem incluir o índice
        dataframe = pd.read_csv(file_path, sep=';', index_col=False)

        # Verifica se a coluna de condição existe no DataFrame
        if condition_column not in dataframe.columns:
            print(f'Erro: Coluna de condição "{condition_column}" não encontrada na tabela')
            return False

        # Converte o valor para string se for um inteiro
        if isinstance(condition_value, int):
            condition_value = str(condition_value)

        # Filtra o DataFrame para manter apenas as linhas onde o valor na coluna de condição é diferente do valor especificado
        dataframe = dataframe[dataframe[condition_column] != condition_value]

        # Remove colunas que contenham "Unnamed:"
        unnamed_columns = [col for col in dataframe.columns if 'Unnamed:' in col]
        dataframe.drop(unnamed_columns, axis=1, inplace=True)

        # Salva o DataFrame resultante no arquivo CSV sem incluir o índice
        dataframe.to_csv(file_path, index=False, sep=';')

        return True

    except FileNotFoundError:
        print('Erro: Tabela não encontrada')
        return False

def update_table(db, table, row_index, updates):
    try:
        file_path = f'./dbs/{db}/{table}.csv'

        # Lê o arquivo CSV em um DataFrame
        dataframe = pd.read_csv(file_path, sep=';', header=None)

        # Verifica se o índice de linha está dentro dos limites
        if not (0 <= row_index < len(dataframe)):
            print('Erro: Índice de linha inválido')
            return False

        # Atualiza os valores nas colunas especificadas para a linha especificada
        for col_index, value in updates.items():
            if 0 <= col_index < len(dataframe.columns):
                dataframe.at[row_index, col_index] = value

        # Salva o DataFrame resultante no arquivo CSV
        dataframe.to_csv(file_path, index=False, header=False, sep=';')

        return True

    except FileNotFoundError:
        print('Erro: Tabela não encontrada')
        return False

# Exemplo de uso insert_into
#insert_into('db_teste', 'teste', ['gabriel', '123', 'gabriel@123'])

# Exemplo de uso update_table
#updates = {1: 'gabriel', 3: 123}
#update_table('db_teste', 'teste', 1, updates)

# Exemplo de uso
#delete_from('db_teste', 'teste', 'Col0', 123)








