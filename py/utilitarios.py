# Autor Jonathan Righi

import platform
from pyspark.sql import SparkSession
from configs.configs_gerais import ConfigsGerais

class Utilitarios:
    
    @staticmethod
    def criar_spark_session(app_name):
        '''
            Funções a serem reutilizadas pelo código
            - A função deverá tentar criar uma sessão Spark, definindo a timezone com base na variável do arquivo
            .env. 
            - Irá verificar o tipo de sistema utilizado, atribuindo corretamente o caminho de onde estão os drivers JDBC.
        '''
        print(f'Iniciando app: {app_name}')
        
        try:        
            configs_gerais = ConfigsGerais()
            configs_gerais.definir_timezone()
            
            if platform.system() == 'Windows':        
                path_jdbc = 'C:\\JDBC\\ojdbc8.jar, C:\\JDBC\\postgresql-42.6.0.jar'
            elif platform.system() == 'Linux':
                path_jdbc = '/opt/jdbc/ojdbc8.jar, /opt/jdbc/postgresql-42.6.0.jar'
                
            print('Criando Sessão Spark. Aguarde um momento...')    
            
            return ( SparkSession.builder
                     .appName(app_name)
                     .config('spark.jars', path_jdbc)
                     .getOrCreate() )
            
        except Exception as e:
            print(f'Erro ao criar sessão Spark para {app_name} {str(e)}')
            raise
