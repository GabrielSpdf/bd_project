import pandas as pd

def insert_into(db, table, values_array):
    if len(values_array) == 0:
        print('Erro: Nenhum valor foi selecionado')
        return False

    try:
        dataframe = pd.read_csv(table)

        for values in values_array:
            dataframe = dataframe.append(values, ignore_index=True)

        dataframe.to_csv(table, index=False)

        return True

    except FileNotFoundError:
        print('Erro: Tabela não encontrada')
        return False

    # insert into teste values (oi, alo, hello)    
