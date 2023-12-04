#_teste    :  uso interno
#__teste :  nome de atribto de classe
#teste_ : evitar conflito com keyword do python
#__teste__ : nome de método especial



# Parametros que serao passados por input do usuario
#conn_params = {
#    'host': localhost
#    'user': root
#    'password': abacaxi123
#    'database': brainwave
#    'port': 3306
#}

from interpreter import interpret

state = 'db_selection'
while state != 'quit':
    while(state == 'db_selection'):
        command = input().split(' ')
        if command[0] == 'use':
            db = command[1]
            state = 'command_reception'
            
        elif command[0] == 'quit':
            state = 'quit'
        
    while(state == 'command_reception'):
        command = input().split(' ')
        if command[0] == 'exit':
            state = 'db_selection'
            continue
        interpret(command, db)
