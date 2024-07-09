# Autor Jonathan Righi

import os
from dotenv import load_dotenv

class ConfigsGerais:
    
    '''    
        Modelos de configurações gerais a serem reutilizadas no código
        Exemplo de uso
        - configs_gerais = ConfigsGerais()
        - configs_gerais.definir_timezone()
        - variaveis_oracle = configs_gerais.conexao_oracle()
    '''
    
    def __init__(self):
        load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

    def definir_timezone(self):
        '''
            Define a timezone com base na variável de ambiente TIMEZONE.
        '''
        try:
            timezone = os.environ.get('TIMEZONE')
            if not timezone:
                raise ValueError('TIMEZONE não está definida nas variáveis de ambiente.')
            os.environ['TZ'] = timezone
            print(f'Timezone definido para: {timezone}')
        
        except Exception as e:
            print(f'Erro ao definir a timezone: Erro ---> {str(e)}')
            raise
        
    def conexao_oracle(self):
        '''
            Retorna as variáveis de conexão para o banco de dados Oracle.
        '''
        try:    
            url = os.environ.get('ORACLE_URL')
            user = os.environ.get('ORACLE_USER')
            driver = os.environ.get('ORACLE_DRIVER')
            password = os.environ.get('ORACLE_PASSWORD')
            
            if not all([url, user, driver, password]):
                raise ValueError('Uma ou mais variáveis de ambiente para conexão Oracle não estão definidas.')
                
            return {
                'url': url,
                'user': user,
                'driver': driver,
                'password': password
            }

        except Exception as e:
            print(f'Erro ao criar ao criar a variavel de conexao_oracle: Erro --> {str(e)}')
            raise

    def conexao_postgres(self):
        '''
            Retorna as variáveis de conexão para o banco de dados Postgres.
        ''' 
        try:
            url = os.environ.get('POSTGRES_URL')
            user = os.environ.get('POSTGRES_USER')
            driver = os.environ.get('POSTGRES_DRIVER')
            password = os.environ.get('POSTGRES_PASSWORD')            
            
            if not all([url, user, driver, password]):
                raise ValueError('Uma ou mais variáveis de ambiente para conexão Postgres não estão definidas.')
        
            return {
                'url': url,
                'user': user,
                'driver': driver,
                'password': password
            }
            
        except Exception as e:
            print(f'Erro ao criar ao criar a variavel de conexao_postgres: Erro --> {str(e)}')
            raise

    def conexao_sqlserver(self):
        '''
            Retorna as variáveis de conexão para o banco de dados SQLServer.
        '''
        try:
            url = os.environ.get('SQLSERVER_URL')
            user = os.environ.get('SQLSERVER_USER')
            driver = os.environ.get('SQLSERVER_DRIVER')
            password = os.environ.get('SQLSERVER_PASSWORD')
            
            if not all([url, user, driver, password]):
                raise ValueError('Uma ou mais variáveis de ambiente para conexão SQLServer não estão definidas.')
            
            return {
                'url': url,
                'user': user,
                'driver': driver,
                'password': password
            }
            
        except Exception as e:
            print(f'Erro ao criar ao criar a variavel de conexao_sqlserver: Erro --> {str(e)}')
            raise
        