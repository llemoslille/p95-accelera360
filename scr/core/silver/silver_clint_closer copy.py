import pandas as pd
import os
import logging
import time

# Cria a pasta logs se não existir
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/silver_clint_closer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

ARQUIVO_BRONZE = 'data/bronze/data/closer.csv'
ARQUIVO_SILVER = 'data/silver/data/silver_clint_closer.parquet'

# Mapeamento das colunas conforme imagem fornecida
colunas_map = {
    'id': 'id',
    'created_at': 'dt_criacao',
    'name': 'nome',
    'email': 'email',
    'ddi': 'ddi',
    'phone': 'fone',
    'complete_phone': 'fone_completo',
    'organization_name': 'de_organizacao',
    'tags': 'tags',
    'status': 'status_atual',
    'stage': 'estagio',
    'value': 'valor',
    'lost_status': 'status_perda',
    'user_email': 'usuario_email',
    'user_name': 'usuario_nome',
    'user_phone': 'usuario_fone',
    'user_link': 'usuario_link',
    'lost_at': 'dt_perda',
    'won_at': 'dt_ganho',
    'valid_phone': 'fl_fone_validado',
    'deal_notes': 'notas_negociacao',
    'employees': 'funcionarios',
    'faturamento': 'faturamento',
    'de_acordo_com_invest': 'de_acordo_com_invest',
    'userId': 'userid',
    'userid': 'userid',
    'role': 'funcao',
    'segment': 'segmento',
    'url': 'url',
    'contact_notes': 'anotacao_contato',
    'user_id': 'usuario_id',
    'city': 'cidade',
    'state': 'uf',
    'currency': 'moeda',
    'duplicate_phone': 'fone_duplicado',
    'acesso_instagram': 'acesso_instagram',
    'user': 'usuario',
}

# Leitura do arquivo bronze
try:
    logger.info(f'Lendo arquivo bronze: {ARQUIVO_BRONZE}')
    bronze_df = pd.read_csv(ARQUIVO_BRONZE)
    logger.info('Arquivo bronze lido com sucesso.')
except Exception as e:
    logger.error(f'Erro ao ler o arquivo bronze: {e}')
    raise

# Renomeando as colunas
bronze_df = bronze_df.rename(columns=colunas_map)
logger.info('Colunas renomeadas.')

# Converter colunas de data para o formato yyyy-mm-dd (apenas data)
colunas_data = ['dt_criacao', 'dt_perda', 'dt_ganho']
for coluna in colunas_data:
    if coluna in bronze_df.columns:
        bronze_df[coluna] = pd.to_datetime(
            bronze_df[coluna], errors='coerce', dayfirst=True
        ).dt.strftime('%Y-%m-%d')

# Lista de colunas extras a serem criadas vazias
colunas_extras = [
'fone_whatsapp',
'documento',
'aplicativo',
'anuncio_id',
'anuncio_nome',
'formulario_id',
'formulario_nome',
'campanha_id',
'campanha_nome',
]

# Garante que todas as colunas extras existam no DataFrame, preenchidas com NaN
for col in colunas_extras:
    if col not in bronze_df.columns:
        bronze_df[col] = pd.NA

# Marca o início do processamento
t_inicio = time.time()
logger.info('Início do processamento.')

# Salvando no formato Parquet (silver)
try:
    bronze_df.to_parquet(ARQUIVO_SILVER, index=False)
    logger.info(f"Arquivo salvo em {ARQUIVO_SILVER} com colunas renomeadas.")
except Exception as e:
    logger.error(f'Erro ao salvar o arquivo silver: {e}')
    raise

# Marca o fim do processamento e calcula a duração
t_fim = time.time()
duracao = t_fim - t_inicio
logger.info(f'Fim do processamento. Duração total: {duracao:.2f} segundos.')
