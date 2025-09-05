import pandas as pd
import time
from datetime import datetime
import logging
import sys
import os

# Configuração do logger


def setup_logger():
    """Configura o logger para o script"""
    # Cria a pasta logs se não existir
    os.makedirs('logs', exist_ok=True)

    log_filename = f'logs/gold_clint_digital_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)


# Inicializa o logger
logger = setup_logger()

# Configurações dos arquivos
ARQUIVO_SILVER = 'gs://p95-accelera360/silver/leads-forms-accelera.parquet'
ARQUIVO_GOLD = 'gs://p95-accelera360/gold/leads-forms-accelera.parquet'


def criar_diretorios():
    """Cria os diretórios necessários se não existirem"""
    diretorios = [
        'data/gold',
        'logs'
    ]

    for diretorio in diretorios:
        if not os.path.exists(diretorio):
            os.makedirs(diretorio, exist_ok=True)
            logger.info(f"Diretório criado: {diretorio}")


def limpar_valores_nulos(df, nome_dataframe=""):
    """
    Substitui todos os valores nan/NaN/None por null (None) no DataFrame
    """
    logger.info(f"Limpando valores nulos em {nome_dataframe}...")

    # Conta valores nulos antes da limpeza
    valores_nulos_antes = df.isnull().sum().sum()

    # Conta strings problemáticas antes
    strings_problematicas_antes = 0
    colunas_texto = df.select_dtypes(include=['object', 'string']).columns

    for col in colunas_texto:
        col_str = df[col].astype(str)
        strings_problematicas_antes += (
            (col_str == 'None') |
            (col_str == 'nan') |
            (col_str == 'NaN') |
            (col_str == 'null') |
            (col_str == 'NULL')
        ).sum()

    # Substitui todos os valores problemáticos por None
    valores_problematicos = ['None', 'nan', 'NaN', 'null', 'NULL']
    df = df.replace(valores_problematicos, None)

    # Conta valores nulos depois da limpeza
    valores_nulos_depois = df.isnull().sum().sum()

    logger.info(f"Valores nulos antes: {valores_nulos_antes}")
    logger.info(f"Strings problemáticas antes: {strings_problematicas_antes}")
    logger.info(f"Valores nulos depois: {valores_nulos_depois}")

    return df


def padronizar_dados(df, nome_dataframe=""):
    """
    Padroniza dados das colunas: remove espaços, deixa maiúsculas e trata valores nan
    """
    logger.info(f"Padronizando dados em {nome_dataframe}...")

    # Conta registros antes da padronização
    registros_antes = len(df)

    # Identifica colunas de texto
    colunas_texto = df.select_dtypes(include=['object', 'string']).columns

    logger.info(f"Colunas de texto encontradas: {list(colunas_texto)}")

    # Para cada coluna de texto
    for col in colunas_texto:
        # Primeiro, trata valores None/nan antes da conversão para string
        df[col] = df[col].replace(['nan', 'NaN', 'null', 'NULL'], None)

        # Remove espaços no início e fim, converte para maiúsculas (apenas valores não-nulos)
        mask_not_null = df[col].notna()
        if mask_not_null.any():
            df.loc[mask_not_null, col] = df.loc[mask_not_null,
                                                col].astype(str).str.strip().str.upper()

        # Substitui valores 'NAN' (string) que podem ter sido criados por None
        df[col] = df[col].replace(['NAN', 'NONE'], None)

    # Conta registros depois da padronização
    registros_depois = len(df)

    logger.info(f"Registros antes: {registros_antes}")
    logger.info(f"Registros depois: {registros_depois}")
    logger.info("Dados padronizados com sucesso")

    return df


def upload_para_gcs_gold(arquivo_local, nome_arquivo_gcs):
    """
    Faz upload de um arquivo para o Google Cloud Storage no bucket gold

    Args:
        arquivo_local (str): Caminho do arquivo local
        nome_arquivo_gcs (str): Nome do arquivo no GCS

    Returns:
        bool: True se o upload foi bem-sucedido, False caso contrário
    """
    try:
        # Ler o arquivo local
        with open(arquivo_local, 'rb') as f:
            arquivo_conteudo = f.read()

        # Fazer upload para o bucket gold
        bucket_path = f"gold/{nome_arquivo_gcs}"
        logger.info(
            f"📤 Fazendo upload para: gs://p95-accelera360/{bucket_path}")

        try:
            from google.cloud import storage
            from google.oauth2 import service_account

            # Caminho para o arquivo de configuração
            config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))))), 'scr', 'config', 'config.yaml')

            # Carregar configuração YAML
            import yaml
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Obter credenciais
            credentials = service_account.Credentials.from_service_account_file(
                config['credentials-path'],
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

            # Criar cliente do Storage
            storage_client = storage.Client(credentials=credentials)
            bucket_name = "p95-accelera360"
            bucket = storage_client.bucket(bucket_name)

            # Nome do arquivo no bucket gold
            nome_arquivo_bucket = f"gold/{nome_arquivo_gcs}"

            logger.info(
                f"📁 Preparando upload para: gs://{bucket_name}/{nome_arquivo_bucket}")

            # Fazer upload
            blob = bucket.blob(nome_arquivo_bucket)
            blob.upload_from_string(
                arquivo_conteudo, content_type='application/octet-stream')

            logger.info(
                f"✅ Upload concluído: gs://{bucket_name}/{nome_arquivo_bucket}")
            return True

        except ImportError:
            logger.error("❌ Módulos GCS não disponíveis")
            return False
        except Exception as e:
            logger.error(f"❌ Erro no upload GCS: {e}")
            return False

    except Exception as e:
        logger.error(f"❌ Erro inesperado no upload para GCS: {e}")
        return False


def main():
    """Função principal do script"""
    start_time_total = time.time()
    logger.info("=== INICIANDO PROCESSAMENTO GOLD CLINT DIGITAL ===")

    try:
        # Cria diretórios necessários
        criar_diretorios()

        # Lê o arquivo Parquet
        logger.info("Lendo arquivo Silver...")
        start_time = time.time()
        df = pd.read_parquet(
            ARQUIVO_SILVER,
            storage_options={
                'token': r'scr\\config\\lille-422512-a12a0a3c757b.json'}
        )
        elapsed_time = time.time() - start_time
        logger.info(f"Arquivo Silver lido em {elapsed_time:.2f} segundos")

        # Exibe informações do DataFrame original
        logger.info(f"Total de registros originais: {len(df)}")
        logger.info(f"Colunas originais: {df.columns.tolist()}")
        logger.info("Primeiras linhas do DataFrame original:")
        print(df.head())

        # Cria o dataframe dim_cliente com as colunas especificadas
        logger.info("Criando dim_cliente...")
        start_time = time.time()

        colunas_dim_cliente = ['nome', 'email', 'ddi', 'fone', 'fone_completo']

        # Verifica se as colunas existem no DataFrame
        colunas_existentes = [
            col for col in colunas_dim_cliente if col in df.columns]
        if len(colunas_existentes) != len(colunas_dim_cliente):
            logger.warning(
                f"Algumas colunas não foram encontradas. Colunas existentes: {colunas_existentes}")
            colunas_dim_cliente = colunas_existentes

        # Cria o dataframe dim_cliente apenas com as colunas necessárias
        dim_cliente = df[colunas_dim_cliente].copy()

        # Trata registros com valores nulos de forma especial
        # Primeiro, separa registros completamente válidos dos que têm algum nulo
        registros_completamente_validos = ~dim_cliente.isnull().any(axis=1)
        dim_cliente_validos = dim_cliente[registros_completamente_validos].copy(
        )
        dim_cliente_com_nulos = dim_cliente[~registros_completamente_validos].copy(
        )

        logger.info(
            f"Registros completamente válidos: {len(dim_cliente_validos)}")
        logger.info(
            f"Registros com algum valor nulo: {len(dim_cliente_com_nulos)}")

        # Remove duplicados dos registros válidos
        dim_cliente_validos = dim_cliente_validos.drop_duplicates()
        logger.info(f"Registros válidos únicos: {len(dim_cliente_validos)}")

        # Para registros com nulos, substitui valores nulos por DESCONHECIDO e remove duplicados
        if len(dim_cliente_com_nulos) > 0:
            logger.info(
                f"Processando {len(dim_cliente_com_nulos)} registros com valores nulos")

            # Substitui valores nulos por valores padrão (mesmos usados no merge do fato)
            dim_cliente_com_nulos['nome'] = dim_cliente_com_nulos['nome'].fillna(
                'DESCONHECIDO_NOME')
            dim_cliente_com_nulos['email'] = dim_cliente_com_nulos['email'].fillna(
                'DESCONHECIDO_EMAIL')
            dim_cliente_com_nulos['ddi'] = dim_cliente_com_nulos['ddi'].fillna(
                'DESCONHECIDO_DDI')
            dim_cliente_com_nulos['fone'] = dim_cliente_com_nulos['fone'].fillna(
                'DESCONHECIDO_FONE')
            dim_cliente_com_nulos['fone_completo'] = dim_cliente_com_nulos['fone_completo'].fillna(
                'DESCONHECIDO_FONE_COMPLETO')

            # Remove duplicados dos registros com nulos tratados
            dim_cliente_com_nulos = dim_cliente_com_nulos.drop_duplicates()
            logger.info(
                f"Registros com nulos únicos após tratamento: {len(dim_cliente_com_nulos)}")

            # Combina registros válidos e registros com nulos tratados
            dim_cliente = pd.concat(
                [dim_cliente_validos, dim_cliente_com_nulos], ignore_index=True)
        else:
            dim_cliente = dim_cliente_validos

        logger.info(
            f"Total de registros únicos em dim_cliente: {len(dim_cliente)}")

        # Gera uma chave primária (PK) para cada linha
        dim_cliente['pk_cliente'] = range(1, len(dim_cliente) + 1)

        # Reorganiza as colunas para que a PK seja a primeira
        colunas_finais = ['pk_cliente'] + colunas_dim_cliente
        dim_cliente = dim_cliente[colunas_finais]

        # Limpa valores nulos no dim_cliente
        dim_cliente = limpar_valores_nulos(dim_cliente, "dim_cliente")

        # Padroniza dados no dim_cliente
        dim_cliente = padronizar_dados(dim_cliente, "dim_cliente")

        elapsed_time = time.time() - start_time
        logger.info(f"Dim_cliente criado em {elapsed_time:.2f} segundos")

        # Exibe informações do DataFrame dim_cliente
        logger.info("=== DIM_CLIENTE ===")
        logger.info(f"Total de registros distintos: {len(dim_cliente)}")
        logger.info(f"Colunas: {dim_cliente.columns.tolist()}")
        print("\nPrimeiras linhas do dim_cliente:")
        print(dim_cliente.head())

        # Salva o dataframe dim_cliente em formato Parquet
        logger.info("Salvando dim_cliente...")
        start_time = time.time()
        arquivo_dim_cliente = 'gs://p95-accelera360/gold/dim_cliente.parquet'
        dim_cliente.to_parquet(
            arquivo_dim_cliente,
            index=False,
            storage_options={
                'token': r'scr\\config\\lille-422512-a12a0a3c757b.json'}
        )
        elapsed_time = time.time() - start_time
        logger.info(
            f"DataFrame dim_cliente salvo em {elapsed_time:.2f} segundos: {arquivo_dim_cliente}")

        # Faz upload do arquivo dim_cliente para GCS
        # Upload não é mais necessário: já gravamos direto no GCS

        # Cria o dataframe dim_vendedores
        logger.info("Criando dim_vendedores...")
        start_time = time.time()

        colunas_dim_vendedores = ['usuario_email',
                                  'usuario_nome', 'usuario_fone', 'usuario_link']

        # Verifica se as colunas existem no DataFrame
        colunas_vendedores_existentes = [
            col for col in colunas_dim_vendedores if col in df.columns]
        if len(colunas_vendedores_existentes) == 0:
            logger.warning(
                "Nenhuma coluna de vendedor encontrada no dataset Silver. Criando dimensão vazia.")
            dim_vendedores = pd.DataFrame(columns=[
                'pk_vendedor', 'vendedor_email', 'vendedor_nome', 'vendedor_fone', 'vendedor_link'
            ])
        else:
            if len(colunas_vendedores_existentes) != len(colunas_dim_vendedores):
                logger.warning(
                    f"Algumas colunas de vendedor não foram encontradas. Colunas existentes: {colunas_vendedores_existentes}")
            colunas_dim_vendedores = colunas_vendedores_existentes

            # Cria o dataframe dim_vendedores apenas com as colunas necessárias
            dim_vendedores = df[colunas_dim_vendedores].copy()

            # Tratar valores 'nan' (string) como nulos nas colunas essenciais (se existirem)
            colunas_essenciais = [c for c in [
                'usuario_email', 'usuario_nome'] if c in dim_vendedores.columns]
            for col in colunas_essenciais:
                dim_vendedores[col] = dim_vendedores[col].replace('nan', None)

            # Trata registros com valores nulos de forma especial
            if len(colunas_essenciais) == 2:
                # Considera apenas colunas essenciais para determinar se é nulo (usuario_email e usuario_nome)
                registros_nulos_vendedor = dim_vendedores[colunas_essenciais].isnull().all(
                    axis=1)
                dim_vendedores_nulos = dim_vendedores[registros_nulos_vendedor].copy(
                )
                dim_vendedores_validos = dim_vendedores[~registros_nulos_vendedor].copy(
                )
            else:
                logger.warning(
                    "Colunas essenciais de vendedor ausentes. Pulando tratamento específico de nulos.")
                dim_vendedores_nulos = pd.DataFrame(
                    columns=dim_vendedores.columns)
                dim_vendedores_validos = dim_vendedores.copy()

        logger.info(
            f"Vendedores com valores nulos: {len(dim_vendedores_nulos)}")
        logger.info(f"Vendedores válidos: {len(dim_vendedores_validos)}")

        # Remove duplicados apenas dos registros válidos
        dim_vendedores_validos = dim_vendedores_validos.drop_duplicates()
        logger.info(
            f"Vendedores válidos únicos: {len(dim_vendedores_validos)}")

        # Para registros nulos, cria APENAS UM registro "DESCONHECIDO"
        if len(dim_vendedores_nulos) > 0:
            logger.info(
                f"Criando um único registro DESCONHECIDO para {len(dim_vendedores_nulos)} vendedores nulos")

            # Cria apenas UM registro para todos os valores nulos
            registro_vendedor_desconhecido = pd.DataFrame({
                col: [f'DESCONHECIDO_{col.upper()}'] for col in colunas_dim_vendedores
            })

            # Combina registros válidos e o único registro nulo
            dim_vendedores = pd.concat(
                [dim_vendedores_validos, registro_vendedor_desconhecido], ignore_index=True)
        else:
            dim_vendedores = dim_vendedores_validos

        logger.info(
            f"Total de registros únicos em dim_vendedores: {len(dim_vendedores)}")

        # Se a dimensão não está vazia, continuar padronização e chaves
        if len(dim_vendedores) > 0:
            # Gera uma chave primária (PK) para cada linha
            dim_vendedores['pk_vendedor'] = range(1, len(dim_vendedores) + 1)

            # Renomeia as colunas de usuario para vendedor
            mapeamento_colunas = {
                'usuario_email': 'vendedor_email',
                'usuario_nome': 'vendedor_nome',
                'usuario_fone': 'vendedor_fone',
                'usuario_link': 'vendedor_link'
            }
            dim_vendedores = dim_vendedores.rename(columns=mapeamento_colunas)

            # Reorganiza as colunas para que a PK seja a primeira (apenas as que existirem)
            colunas_finais_vendedores = ['pk_vendedor'] + [
                mapeamento_colunas[col] for col in colunas_dim_vendedores if col in mapeamento_colunas]
            dim_vendedores = dim_vendedores[colunas_finais_vendedores]

            # Limpa valores nulos no dim_vendedores
            dim_vendedores = limpar_valores_nulos(
                dim_vendedores, "dim_vendedores")

            # Padroniza dados no dim_vendedores
            dim_vendedores = padronizar_dados(dim_vendedores, "dim_vendedores")

        elapsed_time = time.time() - start_time
        logger.info(f"Dim_vendedores criado em {elapsed_time:.2f} segundos")

        # Exibe informações do DataFrame dim_vendedores
        logger.info("=== DIM_VENDEDORES ===")
        logger.info(f"Total de registros distintos: {len(dim_vendedores)}")
        logger.info(f"Colunas: {dim_vendedores.columns.tolist()}")
        print("\nPrimeiras linhas do dim_vendedores:")
        print(dim_vendedores.head())

        # Salva o dataframe dim_vendedores em formato Parquet
        logger.info("Salvando dim_vendedores...")
        start_time = time.time()
        arquivo_dim_vendedores = 'gs://p95-accelera360/gold/dim_vendedores.parquet'
        dim_vendedores.to_parquet(
            arquivo_dim_vendedores,
            index=False,
            storage_options={
                'token': r'scr\\config\\lille-422512-a12a0a3c757b.json'}
        )
        elapsed_time = time.time() - start_time
        logger.info(
            f"DataFrame dim_vendedores salvo em {elapsed_time:.2f} segundos: {arquivo_dim_vendedores}")

        # Faz upload do arquivo dim_vendedores para GCS
        # Upload não é mais necessário: já gravamos direto no GCS

        # Cria o dataframe dim_pipeline
        logger.info("Criando dim_pipeline...")
        start_time = time.time()

        colunas_dim_pipeline = ['de_origem']

        # Verifica se as colunas existem no DataFrame
        colunas_pipeline_existentes = [
            col for col in colunas_dim_pipeline if col in df.columns]
        if len(colunas_pipeline_existentes) != len(colunas_dim_pipeline):
            logger.warning(
                f"Algumas colunas de pipeline não foram encontradas. Colunas existentes: {colunas_pipeline_existentes}")
            colunas_dim_pipeline = colunas_pipeline_existentes

        # Cria o dataframe dim_pipeline apenas com as colunas necessárias
        dim_pipeline = df[colunas_dim_pipeline].copy()

        # Remove registros duplicados
        dim_pipeline = dim_pipeline.drop_duplicates()
        logger.info(f"Registros únicos em dim_pipeline: {len(dim_pipeline)}")

        # Gera uma chave primária (PK) para cada linha
        dim_pipeline['pk_pipeline'] = range(1, len(dim_pipeline) + 1)

        # Renomeia a coluna de_origem para de_pipeline
        dim_pipeline = dim_pipeline.rename(
            columns={'de_origem': 'de_pipeline'})

        # Reorganiza as colunas para que a PK seja a primeira
        colunas_finais_pipeline = ['pk_pipeline', 'de_pipeline']
        dim_pipeline = dim_pipeline[colunas_finais_pipeline]

        # Limpa valores nulos no dim_pipeline
        dim_pipeline = limpar_valores_nulos(dim_pipeline, "dim_pipeline")

        # Padroniza dados no dim_pipeline
        dim_pipeline = padronizar_dados(dim_pipeline, "dim_pipeline")

        elapsed_time = time.time() - start_time
        logger.info(f"Dim_pipeline criado em {elapsed_time:.2f} segundos")

        # Exibe informações do DataFrame dim_pipeline
        logger.info("=== DIM_PIPELINE ===")
        logger.info(f"Total de registros distintos: {len(dim_pipeline)}")
        logger.info(f"Colunas: {dim_pipeline.columns.tolist()}")
        print("\nPrimeiras linhas do dim_pipeline:")
        print(dim_pipeline.head())

        # Salva o dataframe dim_pipeline em formato Parquet
        logger.info("Salvando dim_pipeline...")
        start_time = time.time()
        arquivo_dim_pipeline = 'gs://p95-accelera360/gold/dim_pipeline.parquet'
        dim_pipeline.to_parquet(
            arquivo_dim_pipeline,
            index=False,
            storage_options={
                'token': r'scr\\config\\lille-422512-a12a0a3c757b.json'}
        )
        elapsed_time = time.time() - start_time
        logger.info(
            f"DataFrame dim_pipeline salvo em {elapsed_time:.2f} segundos: {arquivo_dim_pipeline}")

        # Faz upload do arquivo dim_pipeline para GCS
        # Upload não é mais necessário: já gravamos direto no GCS

        # Cria o dataframe dim_estagio
        logger.info("Criando dim_estagio...")
        start_time = time.time()

        colunas_dim_estagio = ['estagio']

        # Verifica se as colunas existem no DataFrame
        colunas_estagio_existentes = [
            col for col in colunas_dim_estagio if col in df.columns]
        if len(colunas_estagio_existentes) != len(colunas_dim_estagio):
            logger.warning(
                f"Algumas colunas de estagio não foram encontradas. Colunas existentes: {colunas_estagio_existentes}")
            colunas_dim_estagio = colunas_estagio_existentes

        # Cria o dataframe dim_estagio apenas com as colunas necessárias
        dim_estagio = df[colunas_dim_estagio].copy()

        # Remove registros duplicados
        dim_estagio = dim_estagio.drop_duplicates()
        logger.info(f"Registros únicos em dim_estagio: {len(dim_estagio)}")

        # Gera uma chave primária (PK) para cada linha
        dim_estagio['pk_estagio'] = range(1, len(dim_estagio) + 1)

        # Renomeia a coluna estagio para de_estagio
        dim_estagio = dim_estagio.rename(columns={'estagio': 'de_estagio'})

        # Reorganiza as colunas para que a PK seja a primeira
        colunas_finais_estagio = ['pk_estagio', 'de_estagio']
        dim_estagio = dim_estagio[colunas_finais_estagio]

        # Limpa valores nulos no dim_estagio
        dim_estagio = limpar_valores_nulos(dim_estagio, "dim_estagio")

        # Padroniza dados no dim_estagio
        dim_estagio = padronizar_dados(dim_estagio, "dim_estagio")

        elapsed_time = time.time() - start_time
        logger.info(f"Dim_estagio criado em {elapsed_time:.2f} segundos")

        # Exibe informações do DataFrame dim_estagio
        logger.info("=== DIM_ESTAGIO ===")
        logger.info(f"Total de registros distintos: {len(dim_estagio)}")
        logger.info(f"Colunas: {dim_estagio.columns.tolist()}")
        print("\nPrimeiras linhas do dim_estagio:")
        print(dim_estagio.head())

        # Salva o dataframe dim_estagio em formato Parquet
        logger.info("Salvando dim_estagio...")
        start_time = time.time()
        arquivo_dim_estagio = 'gs://p95-accelera360/gold/dim_estagio.parquet'
        dim_estagio.to_parquet(
            arquivo_dim_estagio,
            index=False,
            storage_options={
                'token': r'scr\\config\\lille-422512-a12a0a3c757b.json'}
        )
        elapsed_time = time.time() - start_time
        logger.info(
            f"DataFrame dim_estagio salvo em {elapsed_time:.2f} segundos: {arquivo_dim_estagio}")

        # Faz upload do arquivo dim_estagio para GCS
        # Upload não é mais necessário: já gravamos direto no GCS

        # ========================================
        # CRIAÇÃO DA TABELA FATO (APÓS TODAS AS DIMENSÕES)
        # ========================================

        # Cria o dataframe fato_clint_digital
        logger.info("Criando fato_clint_digital...")
        start_time = time.time()

        # Seleciona colunas para o fato (incluindo as necessárias para os merges)
        colunas_fato = [col for col in df.columns if col not in
                        ['ddi', 'fone', 'fone_completo',
                         'usuario_fone', 'usuario_link']]

        # Verifica se as colunas existem no DataFrame
        colunas_fato_existentes = [
            col for col in colunas_fato if col in df.columns]
        if len(colunas_fato_existentes) != len(colunas_fato):
            logger.warning(
                f"Algumas colunas do fato não foram encontradas. Colunas existentes: {colunas_fato_existentes}")
            colunas_fato = colunas_fato_existentes

        # Cria o dataframe fato_clint_digital
        fato_clint_digital = df[colunas_fato].copy()

        # Adiciona chaves estrangeiras para as dimensões
        logger.info("Adicionando chaves estrangeiras...")
        merge_start_time = time.time()

        # Padronizar dados do fato para fazer match com as dimensões
        logger.info("Padronizando dados do fato para merge...")

        # Criar cópias padronizadas das colunas para merge
        def padronizar_coluna_para_merge(coluna, valor_default):
            """Padroniza uma coluna tratando todos os valores nan/None adequadamente"""
            # Primeiro substitui todos os valores problemáticos por None
            coluna_limpa = coluna.replace(
                ['nan', 'NaN', 'null', 'NULL', 'None'], None)
            # Preenche None com valor padrão
            coluna_limpa = coluna_limpa.fillna(valor_default)
            # Padroniza apenas valores não-nulos
            mask_not_null = coluna_limpa.notna()
            if mask_not_null.any():
                coluna_limpa.loc[mask_not_null] = coluna_limpa.loc[mask_not_null].astype(
                    str).str.strip().str.upper()
            # Substitui valores 'NAN' que podem ter sido criados
            coluna_limpa = coluna_limpa.replace(['NAN', 'NONE'], valor_default)
            return coluna_limpa

        if 'nome' in fato_clint_digital.columns:
            fato_clint_digital['nome_padronizado'] = padronizar_coluna_para_merge(
                fato_clint_digital['nome'], 'DESCONHECIDO_NOME')
        if 'email' in fato_clint_digital.columns:
            fato_clint_digital['email_padronizado'] = padronizar_coluna_para_merge(
                fato_clint_digital['email'], 'DESCONHECIDO_EMAIL')
        if 'usuario_email' in fato_clint_digital.columns:
            fato_clint_digital['usuario_email_padronizado'] = padronizar_coluna_para_merge(
                fato_clint_digital['usuario_email'], 'DESCONHECIDO_USUARIO_EMAIL')
        if 'usuario_nome' in fato_clint_digital.columns:
            fato_clint_digital['usuario_nome_padronizado'] = padronizar_coluna_para_merge(
                fato_clint_digital['usuario_nome'], 'DESCONHECIDO_USUARIO_NOME')
        if 'de_origem' in fato_clint_digital.columns:
            fato_clint_digital['de_origem_padronizado'] = padronizar_coluna_para_merge(
                fato_clint_digital['de_origem'], 'DESCONHECIDO_DE_ORIGEM')
        if 'estagio' in fato_clint_digital.columns:
            fato_clint_digital['estagio_padronizado'] = padronizar_coluna_para_merge(
                fato_clint_digital['estagio'], 'DESCONHECIDO_ESTAGIO')

        # Merge com dim_cliente (usando dados padronizados)
        fato_clint_digital = pd.merge(
            fato_clint_digital,
            dim_cliente[['pk_cliente', 'nome', 'email']],
            left_on=['nome_padronizado', 'email_padronizado'],
            right_on=['nome', 'email'],
            how='left',
            suffixes=('', '_dim')
        ).drop(columns=['nome_dim', 'email_dim'])

        # Merge com dim_vendedores (usando dados padronizados)
        fato_clint_digital = pd.merge(
            fato_clint_digital,
            dim_vendedores[['pk_vendedor', 'vendedor_email', 'vendedor_nome']],
            left_on=['usuario_email_padronizado', 'usuario_nome_padronizado'],
            right_on=['vendedor_email', 'vendedor_nome'],
            how='left'
        )

        # Merge com dim_pipeline (usando dados padronizados)
        fato_clint_digital = pd.merge(
            fato_clint_digital,
            dim_pipeline[['pk_pipeline', 'de_pipeline']],
            left_on=['de_origem_padronizado'],
            right_on=['de_pipeline'],
            how='left'
        )

        # Merge com dim_estagio (usando dados padronizados)
        fato_clint_digital = pd.merge(
            fato_clint_digital,
            dim_estagio[['pk_estagio', 'de_estagio']],
            left_on=['estagio_padronizado'],
            right_on=['de_estagio'],
            how='left'
        )

        # Remove colunas duplicadas das dimensões e temporárias (após os merges)
        colunas_para_remover = [
            # Colunas originais duplicadas
            'nome', 'email', 'usuario_email', 'usuario_nome',
            'de_origem', 'estagio', 'vendedor_email', 'vendedor_nome',
            'de_pipeline', 'de_estagio',
            # Colunas temporárias padronizadas
            'nome_padronizado', 'email_padronizado', 'usuario_email_padronizado',
            'usuario_nome_padronizado', 'de_origem_padronizado', 'estagio_padronizado'
        ]

        colunas_para_remover = [
            col for col in colunas_para_remover if col in fato_clint_digital.columns]

        logger.info(
            f"Removendo colunas duplicadas e temporárias: {colunas_para_remover}")
        fato_clint_digital = fato_clint_digital.drop(
            columns=colunas_para_remover)

        # Converte colunas de data para o formato correto
        for col in ['dt_ganho', 'dt_criacao', 'dt_perda']:
            if col in fato_clint_digital.columns:
                fato_clint_digital[col] = pd.to_datetime(
                    fato_clint_digital[col], errors='coerce').dt.date

        merge_elapsed_time = time.time() - merge_start_time
        logger.info(f"Merge realizado em {merge_elapsed_time:.2f} segundos")

        elapsed_time = time.time() - start_time
        logger.info(
            f"Fato_clint_digital criado em {elapsed_time:.2f} segundos")

        # Exibe informações do DataFrame fato_clint_digital
        logger.info("=== FATO_CLINT_DIGITAL ===")
        logger.info(f"Total de registros no fato: {len(fato_clint_digital)}")
        logger.info(f"Colunas: {fato_clint_digital.columns.tolist()}")
        print("\nPrimeiras linhas do fato_clint_digital:")
        print(fato_clint_digital.head())

        # Verifica se há registros sem fk_cliente (merge não encontrou correspondência)
        registros_sem_fk = fato_clint_digital['pk_cliente'].isna().sum()
        if registros_sem_fk > 0:
            logger.warning(
                f"{registros_sem_fk} registros não tiveram correspondência no dim_cliente")
        else:
            logger.info(
                "Todos os registros tiveram correspondência no dim_cliente")

        # Verifica se há registros sem fk_vendedor (merge não encontrou correspondência)
        registros_sem_fk_vendedor = fato_clint_digital['pk_vendedor'].isna(
        ).sum()
        if registros_sem_fk_vendedor > 0:
            logger.warning(
                f"{registros_sem_fk_vendedor} registros não tiveram correspondência no dim_vendedores")
        else:
            logger.info(
                "Todos os registros tiveram correspondência no dim_vendedores")

        # Verifica se há registros sem fk_pipeline (merge não encontrou correspondência)
        registros_sem_fk_pipeline = fato_clint_digital['pk_pipeline'].isna(
        ).sum()
        if registros_sem_fk_pipeline > 0:
            logger.warning(
                f"{registros_sem_fk_pipeline} registros não tiveram correspondência no dim_pipeline")
        else:
            logger.info(
                "Todos os registros tiveram correspondência no dim_pipeline")

        # Verifica se há registros sem fk_estagio (merge não encontrou correspondência)
        registros_sem_fk_estagio = fato_clint_digital['pk_estagio'].isna(
        ).sum()
        if registros_sem_fk_estagio > 0:
            logger.warning(
                f"{registros_sem_fk_estagio} registros não tiveram correspondência no dim_estagio")
        else:
            logger.info(
                "Todos os registros tiveram correspondência no dim_estagio")

        # Salva o dataframe fato_clint_digital em formato Parquet
        logger.info("Salvando fato_clint_digital...")
        start_time = time.time()
        arquivo_fato = 'gs://p95-accelera360/gold/fato_clint_digital.parquet'
        fato_clint_digital.to_parquet(
            arquivo_fato,
            index=False,
            storage_options={
                'token': r'scr\\config\\lille-422512-a12a0a3c757b.json'}
        )
        elapsed_time = time.time() - start_time
        logger.info(
            f"DataFrame fato_clint_digital salvo em {elapsed_time:.2f} segundos: {arquivo_fato}")

        # Faz upload do arquivo fato_clint_digital para GCS
        # Upload não é mais necessário: já gravamos direto no GCS

        # Tempo total de processamento
        total_elapsed_time = time.time() - start_time_total
        logger.info("=== RESUMO DO PROCESSAMENTO ===")
        logger.info(
            f"Tempo total de processamento: {total_elapsed_time:.2f} segundos")
        logger.info(f"Registros originais: {len(df)}")
        logger.info(f"Registros únicos em dim_cliente: {len(dim_cliente)}")
        logger.info(
            f"Registros únicos em dim_vendedores: {len(dim_vendedores)}")
        logger.info(f"Registros únicos em dim_pipeline: {len(dim_pipeline)}")
        logger.info(f"Registros únicos em dim_estagio: {len(dim_estagio)}")
        logger.info(
            f"Registros no fato_clint_digital: {len(fato_clint_digital)}")
        logger.info("=== PROCESSAMENTO CONCLUÍDO COM SUCESSO ===")

    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        raise


if __name__ == "__main__":
    main()
