import requests
from msal import ConfidentialClientApplication
import time
from datetime import timedelta
from loguru import logger
from security_env import (
    pbi_client_id,
    pbi_authority,
    pbi_client_secret,
    pbi_scope,
    pbi_group_id,
    pbi_dataset_id
)
from funcoes import atualizar_modelo_semantico


def main(pasta: str):
    inicio = time.time()

    logger.info("Iniciando atualização do modelo semântico...")

    sucesso = atualizar_modelo_semantico(
        client_id=pbi_client_id,
        authority=pbi_authority,
        client_secret=pbi_client_secret,
        scope=pbi_scope,
        group_id=pbi_group_id,
        dataset_id=pbi_dataset_id
    )

    if sucesso:
        logger.info("Atualização do modelo semântico concluída com sucesso!")
    else:
        logger.error("Falha na atualização do modelo semântico!")

    # Marcar o fim
    fim = time.time()

    # Calcula tempo execução
    tempo_execucao = fim - inicio

    # Converter para horas, minutos e segundos
    tempo_formatado = str(timedelta(seconds=tempo_execucao))

    logger.info(f"TEMPO DE EXECUÇÃO DA REQUISIÇÃO {pasta}: {tempo_formatado}")


if __name__ == "__main__":
    main("gold_atualiza_modelo_semantico")
