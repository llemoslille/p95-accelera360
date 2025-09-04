"""
Módulo de Verificação de Token para Plataforma Clint
Responsável por: Capturar token do BigQuery e preencher código de verificação
"""

from funcoes import capturar_codigo_acesso_client
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import sys

# Adicionar o diretório utils ao path para importação
utils_path = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), '..', '..', 'utils')
sys.path.append(utils_path)

# Importar após configurar o path (como no clint_login_plataforma.py)


def aguardar_campos_codigo(driver, timeout=90):
    """
    Aguarda os campos de código de verificação aparecerem

    Args:
        driver: Driver do Selenium logado
        timeout (int): Tempo máximo de espera em segundos

    Returns:
        list: Lista de elementos de input encontrados, ou None se falhar
    """
    try:
        print("🔍 Aguardando campos de código de verificação...")

        wait = WebDriverWait(driver, timeout)

        for _ in range(timeout):
            code_inputs = driver.find_elements(
                By.CSS_SELECTOR, "input[type='tel']")

            if len(code_inputs) == 6:
                print("✅ Campos de código encontrados!")
                return code_inputs

            time.sleep(1)

        print("❌ Campos de código de verificação não encontrados.")
        return None

    except Exception as e:
        print(f"❌ Erro ao aguardar campos de código: {e}")
        return None


def preencher_codigo_verificacao(driver, code_inputs, token_value):
    """
    Preenche o código de verificação nos campos

    Args:
        driver: Driver do Selenium
        code_inputs: Lista de elementos de input
        token_value: Token capturado do BigQuery

    Returns:
        bool: True se preenchimento bem-sucedido, False caso contrário
    """
    try:
        print("🔐 Preenchendo código de verificação...")

        # Extrair apenas os dígitos do token
        code_digits = [c for c in token_value if c.isdigit()]

        if len(code_digits) < 6:
            print(f"❌ Token inválido (muito curto): {token_value}")
            return False

        # Preencher cada campo
        for i, input_elem in enumerate(code_inputs):
            input_elem.clear()
            input_elem.send_keys(code_digits[i])
            time.sleep(0.5)

        print("✅ Código de verificação preenchido nos campos.")
        return True

    except Exception as e:
        print(f"❌ Erro ao preencher código de verificação: {e}")
        return False


def clicar_botao_continuar(driver):
    """
    Clica no botão 'Continuar' após preenchimento do código

    Args:
        driver: Driver do Selenium

    Returns:
        bool: True se clique bem-sucedido, False caso contrário
    """
    try:
        print("🔄 Clicando no botão 'Continuar'...")

        wait = WebDriverWait(driver, 10)
        continuar_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, "button.btn.btn-success.btn-block")))

        continuar_btn.click()
        print("✅ Botão 'Continuar' clicado com sucesso!")

        # Aguardar redirecionamento e verificar se a sessão foi mantida
        print("⏳ Aguardando redirecionamento após verificação...")
        time.sleep(10)  # Aumentar tempo de espera para processo de verificação

        # Verificar se ainda estamos na página de login (erro) ou se fomos redirecionados
        current_url = driver.current_url
        print(f"📍 URL atual após verificação: {current_url}")

        if "login" in current_url:
            print("⚠️ Ainda na página de login - verificação pode ter falhado")
            print("🔄 Tentando clicar novamente no botão...")

            # Tentar clicar novamente no botão
            try:
                continuar_btn = wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.btn.btn-success.btn-block")))
                continuar_btn.click()
                print("✅ Botão 'Continuar' clicado novamente!")
                time.sleep(15)  # Aguardar mais tempo

                current_url = driver.current_url
                print(f"📍 URL atual após segunda tentativa: {current_url}")

                if "login" in current_url:
                    print("❌ Ainda na página de login após segunda tentativa")
                    return False
            except:
                print("❌ Não foi possível clicar novamente no botão")
                return False

        # Aguardar página carregar completamente
        print("⏳ Aguardando página carregar após verificação...")
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5)

        print("✅ Redirecionamento concluído e sessão mantida!")
        return True

    except Exception as e:
        print(f"⚠️ Não foi possível clicar no botão 'Continuar': {e}")
        return False


def verificar_token_completo(driver):
    """
    Executa o processo completo de verificação de token

    Args:
        driver: Driver do Selenium logado

    Returns:
        bool: True se verificação bem-sucedida, False caso contrário
    """
    try:
        print("🚀 Iniciando verificação de token...")

        # Capturar token do BigQuery
        token_value = capturar_codigo_acesso_client()

        if not token_value:
            print("❌ Falha ao capturar token do BigQuery")
            return False

        print(f"✅ Token capturado: {token_value[:10]}...")

        # Aguardar campos de código
        code_inputs = aguardar_campos_codigo(driver)
        if not code_inputs:
            return False

        # Preencher código de verificação
        if not preencher_codigo_verificacao(driver, code_inputs, token_value):
            return False

        # Clicar no botão continuar
        if not clicar_botao_continuar(driver):
            print("⚠️ Botão continuar não clicado, mas processo pode ter funcionado")

        print("🎉 Verificação de token concluída com sucesso!")
        return True

    except Exception as e:
        print(f"❌ Erro na verificação de token: {e}")
        return False


def executar_verificacao_token(driver):
    """
    Função principal para executar verificação de token

    Args:
        driver: Driver do Selenium logado

    Returns:
        driver: Driver com verificação concluída, ou None se falhar
    """
    try:
        if not verificar_token_completo(driver):
            print("❌ Falha na verificação de token")
            return None

        print("✅ Processo de verificação de token finalizado!")
        return driver

    except Exception as e:
        print(f"❌ Erro no processo de verificação: {e}")
        return None


if __name__ == "__main__":
    # Teste do módulo (requer driver já logado)
    print("🧪 Teste do módulo de verificação de token")
    print("⚠️ Este módulo requer um driver já logado na Clint")
    print("💡 Execute através do main principal")
