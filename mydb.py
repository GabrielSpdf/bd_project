import os
import csv
import re

class DataBase:
    def __init__(self):
        self.banco_dados_interno = {}

    def importar_csv(self, diretorio):
        for arquivo in os.listdir(diretorio):
            if arquivo.endswith('.csv'):
                tabela = os.path.splitext(arquivo)[0]
                with open(os.path.join(diretorio, arquivo), 'r') as arquivo_csv:
                    leitor_csv = csv.DictReader(arquivo_csv)
                    dados = [linha for linha in leitor_csv]
                    self.banco_dados_interno[tabela] = dados

    def criar_tabela(self, tabela, colunas):
        # Verifica se a tabela já existe
        if tabela not in self.banco_dados_interno:
            self.banco_dados_interno[tabela] = []

    def inserir_dados(self, tabela, colunas, dados):
        # Adiciona os dados à tabela
        if tabela in self.banco_dados_interno:
            self.banco_dados_interno[tabela].extend(dados)

    def consultar_dados(self, tabela, projetar=None, filtros=None, ordenar=None):
        # Implementação simples da consulta
        dados = self.banco_dados_interno.get(tabela, [])

        if filtros:
            dados = self.aplicar_filtro(dados, filtros)

        if projetar:
            dados = self.aplicar_projetar(dados, projetar)

        if ordenar:
            dados = self.aplicar_ordenacao(dados, ordenar)

        return dados

    def aplicar_filtro(self, dados, filtros):
        # Aplica filtros aos dados
        resultado = []
        for linha in dados:
            if all(linha[coluna] == valor for coluna, valor in filtros.items()):
                resultado.append(linha)
        return resultado

    def aplicar_projetar(self, dados, colunas):
        # Retorna apenas as colunas desejadas
        return [{coluna: linha[coluna] for coluna in colunas} for linha in dados]

    def aplicar_ordenacao(self, dados, ordenar):
        # Ordena os dados
        return sorted(dados, key=lambda x: tuple(x[coluna] for coluna in ordenar))


# Exemplo de uso da classe
ferramenta = DataBase()

# Importar dados de CSV
ferramenta.importar_csv('D:\Dev\CSV')




