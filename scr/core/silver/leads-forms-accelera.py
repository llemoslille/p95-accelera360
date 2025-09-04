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

ARQUIVO_BRONZE = 'data/bronze/leads-forms-accelera/leads-forms-accelera.csv'
ARQUIVO_SILVER = 'data/silver/leads-forms-accelera.parquet'

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
    'origin': 'de_origem',
    'won_at': 'dt_ganho',
    'lost_at': 'dt_perda',
    'doc': 'documento',
    'username': 'username',
    'valid_phone': 'fone_validado',
    'area_de_atuacao': 'area_de_atuacao',
    'principais_desafios': 'principais_desafios',
    'como_ficou_sabendo_d': 'como_ficou_sabendo_d',
    'disposicao_de_invest': 'disposicao_de_invest',
    'media_de_faturamento': 'media_de_faturamento',
    'meta_para_os_proximo': 'meta_para_os_proximo',
    'momento_atual_na_jor': 'momento_atual_na_jor',
    'o_que_voce_espera_co': 'o_que_voce_espera_co',
    'link_do_forms': 'link_do_forms',
    'meta_de_faturamento': 'meta_de_faturamento',
    'perfil_profissional': 'perfil_profissional',
    'para_crescer_um_nego': 'para_crescer_um_nego',
    'principal_objetivo_h': 'principal_objetivo_h',
    'qual_e_o_maior_obsta': 'qual_e_o_maior_obsta',
    'qual_e_sua_prioridad': 'qual_e_sua_prioridad',
    'sua_posicao_no_merca': 'sua_posicao_no_merca',
    'deal_notes': 'notas_negociacao',
    'duplicate_phone': 'fone_duplicado',
    'whatsapp_number': 'whatsapp',
    'user': 'user',
    'currency': 'moeda',
    'user_id': 'userId',
    'instagram': 'instagram',
    'contact_notes': 'notas_contato',
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

# Marca o início do processamento
t_inicio = time.time()
logger.info('Início do processamento.')
# Função para remover quebras de linha de todas as colunas de texto


def remover_quebras_linha(df):
    """
    Remove quebras de linha de todas as colunas de texto do DataFrame

    Args:
        df: DataFrame do pandas

    Returns:
        DataFrame com quebras de linha removidas
    """
    logger.info('Removendo quebras de linha das colunas de texto...')

    for coluna in df.columns:
        if df[coluna].dtype == 'object':  # Colunas de texto
            df[coluna] = df[coluna].astype(
                str).str.replace('\n', ' ', regex=False)
            df[coluna] = df[coluna].str.replace('\r', ' ', regex=False)
            df[coluna] = df[coluna].str.replace(
                '  ', ' ', regex=True)  # Remove espaços duplos
            # Remove espaços no início e fim
            df[coluna] = df[coluna].str.strip()

    logger.info('Quebras de linha removidas com sucesso.')
    return df


# Aplicar a função para remover quebras de linha
bronze_df = remover_quebras_linha(bronze_df)

# Salvando no formato Parquet (silver)
try:
    # Criar diretório se não existir
    os.makedirs(os.path.dirname(ARQUIVO_SILVER), exist_ok=True)

    bronze_df.to_parquet(ARQUIVO_SILVER, index=False)
    logger.info(f"Arquivo salvo em {ARQUIVO_SILVER} com colunas renomeadas.")
except Exception as e:
    logger.error(f'Erro ao salvar o arquivo silver: {e}')
    raise

# Marca o fim do processamento e calcula a duração
t_fim = time.time()
duracao = t_fim - t_inicio
logger.info(f'Fim do processamento. Duração total: {duracao:.2f} segundos.')
