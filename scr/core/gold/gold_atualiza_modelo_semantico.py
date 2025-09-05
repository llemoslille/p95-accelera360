import requests
from msal import ConfidentialClientApplication
import time
from datetime import timedelta
import logging
import os
import sys

# Import robusto das variáveis (tenta absoluto e depois ajusta sys.path)
try:
    from scr.utils.variaveis import (
        pbi_client_id,
        pbi_authority,
        pbi_client_secret,
        pbi_scope,
        pbi_group_id,
        pbi_dataset_id,
    )
except Exception:
    utils_path = os.path.join(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__)))), 'utils')
    if utils_path not in sys.path:
        sys.path.append(utils_path)
    from variaveis import (
        pbi_client_id,
        pbi_authority,
        pbi_client_secret,
        pbi_scope,
        pbi_group_id,
        pbi_dataset_id,
    )

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def atualizar_modelo_semantico(client_id, authority, client_secret, scope, group_id, dataset_id):
    """
    Função para atualizar o modelo semântico do Power BI.
    """
    try:
        logger.info("Iniciando autenticação com Microsoft Graph...")

        # Autenticação
        app = ConfidentialClientApplication(
            client_id=client_id,
            authority=authority,
            client_credential=client_secret
        )

        token_response = app.acquire_token_for_client(scopes=scope)
        if 'access_token' not in token_response:
            logger.error(
                f"Falha ao obter token de acesso: {token_response.get('error_description', 'Erro desconhecido')}")
            return False

        access_token = token_response['access_token']
        logger.info("Token de acesso obtido com sucesso!")

        # Chamada para atualizar o dataset
        refresh_url = f'https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes'
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }

        logger.info(f"Iniciando refresh do dataset {dataset_id}...")
        response = requests.post(refresh_url, headers=headers)

        if response.status_code == 202:
            logger.info(
                '✅ Refresh iniciado com sucesso! Status: 202 (Accepted)')
            return True
        else:
            logger.error(
                f'❌ Erro ao iniciar refresh: Status {response.status_code}')
            logger.error(f'Resposta: {response.text}')
            return False

    except Exception as e:
        logger.error(f"❌ Erro durante a atualização: {str(e)}")
        return False


def main():
    inicio = time.time()

    logger.info("🚀 Iniciando atualização do modelo semântico do Power BI...")

    # Configurações do Power BI - credenciais hardcoded para funcionar no Cloud Run
    # TODO: Migrar para variáveis de ambiente em produção
    # Monta config usando variáveis importadas (sem usar o nome do módulo)
    config = {
        'client_id': pbi_client_id,
        'authority': pbi_authority,
        'client_secret': pbi_client_secret,
        'scope': [pbi_scope] if isinstance(pbi_scope, str) else pbi_scope,
        'group_id': pbi_group_id,
        'dataset_id': pbi_dataset_id
    }

    # Normaliza authority removendo possíveis prefixos f'..', aspas e barra final
    try:
        auth = str(config.get('authority') or '').strip()
        if (auth.startswith("f'") and auth.endswith("'")) or (auth.startswith('f"') and auth.endswith('"')):
            auth = auth[2:-1]
        auth = auth.strip("'\"").strip()
        auth = auth.rstrip('/')
        config['authority'] = auth
    except Exception:
        pass

    logger.info("🔐 Usando credenciais configuradas para Power BI")
    logger.info(f"🔑 Authority: {config.get('authority')}")
    logger.info(f"📊 Grupo: {config['group_id']}")
    logger.info(f"📈 Dataset: {config['dataset_id']}")

    # Executar atualização
    sucesso = atualizar_modelo_semantico(**config)

    if sucesso:
        logger.info("✅ Atualização do modelo semântico concluída com sucesso!")
    else:
        logger.error("❌ Falha na atualização do modelo semântico!")

    # Calcular tempo de execução
    fim = time.time()
    tempo_execucao = fim - inicio
    tempo_formatado = str(timedelta(seconds=tempo_execucao))

    logger.info(f"⏱️ Tempo de execução: {tempo_formatado}")


if __name__ == "__main__":
    main()
