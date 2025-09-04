"""
Módulo de Login para Plataforma Clint
Responsável por: Acesso, login básico e contagem regressiva
"""

from variaveis import clint_url, clint_user, clint_password
from funcoes import contagem_regressiva
from selenium import webdriver
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


def criar_driver_chrome():
    """
    Cria e configura o driver do Chrome para automação
    """
    try:
        print("🌐 Configurando navegador Chrome...")

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument(
            "--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option(
            "excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        # Configurar pasta de downloads personalizada
        downloads_path = r"C:\Repositorio\Python\p95-accelera360\data\bronze\leads-forms-accelera"
        os.makedirs(downloads_path, exist_ok=True)

        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": downloads_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        })

        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        print("✅ Driver Chrome criado com sucesso!")
        return driver

    except Exception as e:
        print(f"❌ Erro ao criar driver Chrome: {e}")
        return None


def fazer_login(driver):
    """
    Realiza o login na plataforma Clint

    Args:
        driver: Driver do Selenium configurado

    Returns:
        bool: True se login bem-sucedido, False caso contrário
    """
    try:
        print("🚀 Iniciando processo de login na Clint...")

        # Verificar se as variáveis estão configuradas
        if not clint_url or not clint_user or not clint_password:
            print("❌ Variáveis de ambiente não configuradas")
            return False

        print(f"✅ URL da Clint: {clint_url}")
        print(f"✅ Usuário: {clint_user}")

        wait = WebDriverWait(driver, 30)

        # Acessar o site da Clint
        print(f"🔗 Acessando: {clint_url}")
        driver.get(clint_url)
        time.sleep(3)

        print("✅ Site da Clint carregado com sucesso!")
        print(f"📄 Título da página: {driver.title}")

        # Aguardar página de login carregar
        print("⏳ Aguardando página de login carregar...")
        time.sleep(3)

        # Realizar login automático
        print("🔐 Iniciando processo de login...")

        # Campo de email
        email_field = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input[placeholder='Email']")))
        print("✅ Campo de email encontrado")

        email_field.clear()
        email_field.send_keys(clint_user)
        print(f"✅ Email preenchido: {clint_user}")
        time.sleep(1)

        # Campo de senha
        password_field = wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "input[placeholder='Senha']")))
        print("✅ Campo de senha encontrado")

        password_field.clear()
        password_field.send_keys(clint_password)
        print("✅ Senha preenchida")
        time.sleep(1)

        # Botão de continuar
        continue_button = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(text(), 'Continuar')]")))
        continue_button.click()
        print("✅ Botão de continuar clicado")

        # Aguardar processamento do login
        print("⏳ Aguardando processamento do login...")
        time.sleep(5)

        print("✅ Login realizado com sucesso!")
        return True

    except Exception as e:
        print(f"❌ Erro durante o login: {e}")
        return False


def aguardar_contagem_regressiva(segundos=45):
    """
    Executa contagem regressiva após login

    Args:
        segundos (int): Tempo em segundos para contagem
    """
    try:
        print("⏳ Iniciando contagem regressiva...")
        contagem_regressiva(segundos)
        print("✅ Contagem regressiva concluída!")

    except Exception as e:
        print(f"⚠️ Erro na contagem regressiva: {e}")
        print("🔄 Usando contador padrão...")

        # Contador padrão como fallback
        for i in range(segundos, 0, -1):
            print(f"\rTempo restante: {i:3d} segundos", end="", flush=True)
            time.sleep(1)
        print("\n✅ Tempo de espera concluído!")


def executar_login_completo():
    """
    Executa o processo completo de login

    Returns:
        driver: Driver configurado e logado, ou None se falhar
    """
    driver = None

    try:
        # Criar driver
        driver = criar_driver_chrome()
        if not driver:
            return None

        # Fazer login
        if not fazer_login(driver):
            print("❌ Falha no login")
            return None

        # Aguardar contagem regressiva
        aguardar_contagem_regressiva(60)

        print("🎉 Processo de login completo finalizado com sucesso!")
        return driver

    except Exception as e:
        print(f"❌ Erro no processo de login: {e}")
        if driver:
            driver.quit()
        return None


if __name__ == "__main__":
    # Teste do módulo
    print("🧪 Testando módulo de login...")
    driver = executar_login_completo()

    if driver:
        print("✅ Teste bem-sucedido! Driver retornado.")
        input("Pressione ENTER para fechar o navegador...")
        driver.quit()
    else:
        print("❌ Teste falhou!")
