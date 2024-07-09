# Autor Jonathan Righi
# ETL - Relatorio de Ouvidorias Geradas no MV desde 2022

import os
import platform
from py.utilitarios import Utilitarios
from connection.database import ConexaoDatabase
from configs.configs_gerais import ConfigsGerais

if __name__ == "__main__":
    try:        
        
        '''
            - Modelo de ETL a ser utilizado com conexoes e utilitarios, demais funções devem ser escritos
            - O parâmetro how='left_anti' indica que estou selecionando todas as linhas de dados_ouvidorias_mv que não têm correspondência em dados_ouvidorias_cloud.
        '''

        # Utilizando a classe Utilitarios para definir nome do aplicativo e iniciar uma sessão Spark
        app_name = 'Nome APP'
        spark = Utilitarios.criar_spark_session(app_name)        
        
        # Definindo instancias de ConfigsGerais e obtendo variáveis de conexões com os banco de dados
        print('Obtendo variáveis de conexões, por favor, aguarde...')
        configs_gerais = ConfigsGerais()        
        conexao_oracle = configs_gerais.conexao_oracle()
        conexao_postgres = configs_gerais.conexao_postgres()
        conexao_sqlserver = configs_gerais.conexao_sqlserver()    
        
        if platform.system() == 'Windows':
            path_sql = os.path.join(os.path.dirname(__file__), 'sql\\') 
        elif platform.system() == 'Linux':
            path_sql = os.path.join(os.path.dirname(__file__), 'sql/') 
            
        # Instanciando conexões com Databases
        database_oracle = ConexaoDatabase(conexao_oracle)
        database_postgres = ConexaoDatabase(conexao_postgres)
        database_sqlserver = ConexaoDatabase(conexao_sqlserver)

        '''
            DEFINIDO ATE AQUI 
            - TIMEZONES
            - ALTERAR NOME DO APLICATIVO
            - CONEXOES COM DATABASES (ALTERAR SE NECESSARIO, INFORMAR USER, SENHA E IP)
            - VARIAVEL DO CAMINHO SQL COM BASE NA PLATAFORMA UTILIZADA
            
        '''
        
    except Exception as e:
        print(f'Erro durante a inicialização do programa: {str(e)}')
        
    finally:
        print('Rotina finalizada.')
        spark.stop()
