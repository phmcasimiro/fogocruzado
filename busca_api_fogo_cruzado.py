 
# lendo libraries
import requests as re
from datetime import date, timedelta
import pandas as pd
from sqlalchemy import create_engine

 
# parâmetros da requisição
par = {'email':'mateus.evangelista.alcantara@gmail.com',
       'password':'Ma2442109'}
# post para ter o token
req = re.post('https://api-service.fogocruzado.org.br/api/v2/auth/login', par)
token = req.json()['data']['accessToken']
token

 
def parametros(n_pagina:int = 1) -> dict:
       """Função que retorna um dicionário de parâmetros para métodos get da api
       param n_pagina: número de páginas que a resposta da api tem. Valor-padrão: 1. Assume-se que esta é a quantidade de páginas
       returns: dicionário com parâmetros da requisição
       """
       id_rj = 'b112ffbe-17b3-4ad0-8f2a-2038745d1d14'
       par = {'idState':id_rj,
       'page':n_pagina, 'finaldate':date.today(),
       'initialdate':date.today() - timedelta(days=6)}
       return par

 
# quantidade de páginas da resposta
qtd_paginas = re.get('https://api-service.fogocruzado.org.br/api/v2/occurrences', headers={'Authorization':f'Bearer {token}'},
       params=parametros()).json()['pageMeta']['pageCount']

# lista vazia à qual serão adicionados dados
dados = []

# se tem uma página, não precisa iterar
if qtd_paginas == 1:
       dados = re.get('https://api-service.fogocruzado.org.br/api/v2/occurrences', headers={'Authorization':f'Bearer {token}'},
                      params=parametros()).json()['data']
else:       
       for i in range(1, qtd_paginas+1):
              dados += re.get('https://api-service.fogocruzado.org.br/api/v2/occurrences', headers={'Authorization':f'Bearer {token}'},
              params=parametros(i)).json()['data']
dados

 
# removendo valores importantes de json para colocar dentro da própria chave
# por exemplo city: {name: 'Belford Roxo', id: 1234}. Apenas o nome vale. Essa lógica serve pros outros
for ocorrencia in dados:
   ocorrencia['city'] =ocorrencia['city']['name']
   ocorrencia['neighborhood'] =ocorrencia['neighborhood']['name']
   if ocorrencia['subNeighborhood'] is not None:
       ocorrencia['subNeighborhood'] = ocorrencia['subNeighborhood']['name']
   ocorrencia['latitude'] =ocorrencia['latitude'].replace('\t', '')
   ocorrencia['longitude'] =ocorrencia['longitude'].replace('\t','')
   if ocorrencia['locality'] is not None:
       ocorrencia['locality'] =ocorrencia['locality']['name']
   ocorrencia['contextInfo']['mainReason']['name']
   ocorrencia['contextInfo']['massacre']
   ocorrencia['contextInfo']['policeUnit']
   txt = ', '.join([clip['name'] for clip in ocorrencia['contextInfo']['clippings']])
   if txt == '':
       ocorrencia['clippings'] = None
   else:
       ocorrencia['clippings'] = txt 
   txt = ', '.join([comp_reason['name'] for comp_reason in ocorrencia['contextInfo']['complementaryReasons']])
   if txt == '':
       ocorrencia['complementaryReasons'] = "Não identificado"
   else:
       ocorrencia['complementaryReasons'] = txt
    # conta quantas mortes ou feridos houve por ocorrencia
   ocorrencia['total_mortos'] = len([dado['situation'] for dado in ocorrencia['victims'] if dado['situation'] == 'Dead'])
   ocorrencia['total_feridos'] = len([dado['situation'] for dado in ocorrencia['victims'] if dado['situation'] == 'Wounded'])
   
   # deletando o que não é dicionário dentro de dicionário ou o que não é útil
   del ocorrencia['state']
   del ocorrencia['contextInfo']
   del ocorrencia['region']
   del ocorrencia['animalVictims']
   del ocorrencia['transports']
   del ocorrencia['victims']
   del ocorrencia['relatedRecord']

dados

 
# cria dicionário vazio com as chaves dos dados, que serão colunas do dataframe
dicionario_final = {chave:[] for chave in dados[0].keys()}
dicionario_final

 
# pega a entrada de cada dicionario da lista dados e cria uma nova lista com a mesma chave
for dado in dados:
    for chave in dicionario_final.keys():
        dicionario_final[chave].append(dado[chave])
dicionario_final

 
# cria dataframe
df = pd.DataFrame(dicionario_final)
df

 
# alterando tipo que não foram lidos corretamente
df['latitude'] = df['latitude'].astype('float32')
df['longitude'] = df['longitude'].astype('float32')
df['date'] = pd.to_datetime(df['date'])
df

 
# dados do banco de dados
senha = input("Digite a senha: ")
senha = senha.replace('@', '%40') # para evitar erro na conn_string
conn_string = 'postgresql://opengeo:{}@d-pgsql01.pgj.rj.gov.br/opengeo'.format(senha)

engine = create_engine(conn_string)

 
# envia ao BD
df.to_sql('apifogocruzado_staging', engine, schema='basegeo', if_exists='append', index=False)


