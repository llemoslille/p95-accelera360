import os
import yaml
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import sys
import time

# Adicionar o diretório config ao path
sys.path.append(os.path.join(os.path.dirname(
    os.path.abspath(__file__)), '..', 'config'))


def contagem_regressiva(segundos):
    """
    Executa uma contagem regressiva com formatação visual

    Args:
        segundos (int): Tempo total em segundos para a contagem regressiva
    """
    try:
        print(f"⏳ Iniciando contagem regressiva de {segundos} segundos...")

        for i in range(segundos, 0, -1):
            # Formatação com emojis e cores
            if i > 60:
                minutos = i // 60
                segs = i % 60
                print(
                    f"\r⏰ Tempo restante: {minutos:2d}:{segs:02d} minutos", end="", flush=True)
            elif i > 10:
                print(
                    f"\r⏰ Tempo restante: {i:3d} segundos", end="", flush=True)
            else:
                print(
                    f"\r🚨 Tempo restante: {i:3d} segundos", end="", flush=True)

            time.sleep(1)

        print("\n✅ Contagem regressiva concluída!")

    except Exception as e:
        print(f"❌ Erro na contagem regressiva: {e}")


def carregar_configuracao():
    """
    Carrega as configurações do arquivo config.yaml
    """
    # Caminho para o arquivo config.yaml a partir do diretório raiz
    workspace_root = os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(workspace_root, 'scr', 'config', 'config.yaml')

    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        print("✅ Configuração carregada com sucesso")
        return config
    except Exception as e:
        print(f"❌ Erro ao carregar configuração: {e}")
        return None


def obter_credenciais_bigquery(config):
    """
    Obtém as credenciais do BigQuery
    """
    try:
        credentials_path = config.get('credentials-path')

        if not credentials_path or not os.path.exists(credentials_path):
            print(
                f"❌ Arquivo de credenciais não encontrado: {credentials_path}")
            return None

        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        print("✅ Credenciais do BigQuery carregadas com sucesso")
        return credentials

    except Exception as e:
        print(f"❌ Erro ao carregar credenciais: {e}")
        return None


def capturar_codigo_acesso_client():
    """
    Captura o código de acesso do client usando a query do BigQuery
    """
    try:
        print("🚀 Iniciando captura do código de acesso do client...")

        # Carregar configuração
        config = carregar_configuracao()
        if not config:
            return None

        # Obter credenciais
        credentials = obter_credenciais_bigquery(config)
        if not credentials:
            return None

        # Criar cliente BigQuery
        client = bigquery.Client(
            credentials=credentials,
            project=config['project-id']
        )
        print(
            f"✅ Cliente BigQuery criado para projeto: {config['project-id']}")

        # Query para capturar o código de acesso
        query = """
        WITH MAXDATE AS (
          SELECT
            MAX(data_token) AS max_data
          FROM `lille-422512.P95_Accelera360.validacao_token`  
        )
        SELECT 
          token
        FROM `lille-422512.P95_Accelera360.validacao_token` vt
        CROSS JOIN MAXDATE
        WHERE vt.data_token = MAXDATE.max_data
        """

        print("🔍 Executando query no BigQuery...")

        # Executar query
        query_job = client.query(query)
        results = query_job.result()

        # Converter resultados para DataFrame
        df = results.to_dataframe()

        if df.empty:
            print("⚠️ Nenhum resultado encontrado na query")
            return None

        # Obter o token (primeira linha)
        token = df.iloc[0]['token']

        print(f"✅ Código de acesso capturado com sucesso: {token[:10]}...")
        return token

    except Exception as e:
        print(f"❌ Erro ao capturar código de acesso: {e}")
        return None


def testar_conexao_bigquery():
    """
    Testa a conexão com o BigQuery
    """
    try:
        print("🧪 Testando conexão com BigQuery...")

        # Carregar configuração
        config = carregar_configuracao()
        if not config:
            return False

        # Obter credenciais
        credentials = obter_credenciais_bigquery(config)
        if not credentials:
            return False

        # Criar cliente BigQuery
        client = bigquery.Client(
            credentials=credentials,
            project=config['project-id']
        )

        # Testar conexão com uma query simples
        query = f"SELECT 1 as test FROM `{config['project-id']}.P95_Accelera360.validacao_token` LIMIT 1"
        query_job = client.query(query)
        results = query_job.result()

        print("✅ Conexão com BigQuery testada com sucesso!")
        return True

    except Exception as e:
        print(f"❌ Erro ao testar conexão: {e}")
        return False


if __name__ == "__main__":
    # Testar a função
    print("🧪 Testando funções do módulo...")

    # Testar conexão
    if testar_conexao_bigquery():
        # Capturar código de acesso
        token = capturar_codigo_acesso_client()
        if token:
            print(f"🎯 Token capturado: {token}")
        else:
            print("❌ Falha ao capturar token")
    else:
        print("❌ Falha na conexão com BigQuery")
