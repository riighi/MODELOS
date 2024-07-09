# Autor Jonathan Righi 

class ConexaoDatabase:
    
    '''
        * * * Conexao Database * * *
        Exemplo de uso
        - Instanciar a classe ConexaoDatabase: conexao_database = ConexaoDatabase(dados_conexao)
        - df = conexao_database.select_database(spark, 'caminho/para/arquivo.sql')
        - insert_database(dados, nome_tabela)
    '''
    
    def __init__(self, dados_conexao):
        self.url = dados_conexao['url']
        self.user = dados_conexao['user']
        self.driver = dados_conexao['driver']
        self.password = dados_conexao['password']
        print(f'Efetuando conexao com {self.driver}, por favor, aguarde...')

    def select_database(self, spark, sql_file):
        try:
            with open(sql_file, 'r') as arquivo:
                conteudo_sql = arquivo.read()
                return (spark.read
                        .format('jdbc')
                        .option('url', self.url)
                        .option('dbtable', f'({conteudo_sql}) consulta')
                        .option('user', self.user)
                        .option('password', self.password)
                        .option('driver', self.driver)
                        .load())
        except Exception as e:
            print(f'Erro ao ler dados do {self.driver}: {str(e)}')
            
    def insert_database(self, dados, nome_tabela):
        try:
            print(f'Inserindo dados em {self.driver}, por favor, aguarde...')
            (dados.write
                .format('jdbc')
                .option('url', self.url)
                .option('dbtable', nome_tabela)
                .option('user', self.user) 
                .option('password', self.password) 
                .option('driver', self.driver) 
                .mode('append')
                .save()
                )

        except Exception as e:
            print(f'Erro ao inserir dados no {self.driver}: {str(e)}')
            
