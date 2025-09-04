import sys
import os

# Adicionar o diretório config ao path para importação
sys.path.append(os.path.join(os.path.dirname(
    os.path.abspath(__file__)), '..', 'config'))

# Imports com tratamento de erro
try:
    import pandas as pd
    print("✅ pandas importado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar pandas: {e}")
    pd = None

try:
    import re
    print("✅ re importado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar re: {e}")
    re = None

try:
    import time
    print("✅ time importado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar time: {e}")
    time = None

try:
    from selenium.webdriver.support import expected_conditions as EC
    print("✅ selenium.webdriver.support.expected_conditions importado com sucesso")
except ImportError as e:
    print(
        f"❌ Erro ao importar selenium.webdriver.support.expected_conditions: {e}")
    EC = None

try:
    from selenium.webdriver.support.ui import WebDriverWait
    print("✅ selenium.webdriver.support.ui.WebDriverWait importado com sucesso")
except ImportError as e:
    print(
        f"❌ Erro ao importar selenium.webdriver.support.ui.WebDriverWait: {e}")
    WebDriverWait = None

try:
    from selenium.webdriver.common.by import By
    print("✅ selenium.webdriver.common.by.By importado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar selenium.webdriver.common.by.By: {e}")
    By = None

try:
    from selenium import webdriver
    print("✅ selenium.webdriver importado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar selenium.webdriver: {e}")
    webdriver = None

try:
    from gcs_credentials import GCSConfig
    print("✅ gcs_credentials.GCSConfig importado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar gcs_credentials.GCSConfig: {e}")
    GCSConfig = None

try:
    from config import ClintConfig
    print("✅ config.ClintConfig importado com sucesso")
except ImportError as e:
    print(f"❌ Erro ao importar config.ClintConfig: {e}")
    ClintConfig = None

# Verificar se todos os módulos essenciais foram importados
if not all([pd, re, time, EC, WebDriverWait, By, webdriver, GCSConfig, ClintConfig]):
    print("❌ Alguns módulos essenciais não puderam ser importados. Verifique as dependências.")
    sys.exit(1)

print("✅ Todos os módulos foram importados com sucesso!")


# Adicionar o diretório scripts ao path para importação
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def main():
    # Criar pasta downloads se não existir
    downloads_path = ClintConfig.get_downloads_path()
    os.makedirs(downloads_path, exist_ok=True)

    # Configurar Chrome para usar pasta downloads local
    chrome_options = ClintConfig.get_chrome_options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": os.path.abspath(downloads_path),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=chrome_options)
    driver.maximize_window()  # Maximiza a janela do navegador
    wait = WebDriverWait(driver, 30)
    try:
        # Login Clint
        driver.get(ClintConfig.CLINT_URL)
        print("Aba 1: Clint aberta.")
        time.sleep(2)
        user_field = driver.find_element(By.NAME, "email")
        user_field.clear()
        user_field.send_keys(ClintConfig.EMAIL)
        print("Usuário preenchido.")
        time.sleep(1)
        pass_field = driver.find_element(By.NAME, "password")
        pass_field.clear()
        pass_field.send_keys(ClintConfig.PASSWORD)
        print("Senha preenchida.")
        time.sleep(1)
        login_btn = driver.find_element(By.XPATH, "//button[@type='submit']")
        login_btn.click()
        print("Botão de login clicado.")
        # Contador regressivo de 120 segundos
        countdown_time = 45
        print(f"Aguardando {countdown_time} segundos...")
        for i in range(countdown_time, 0, -1):
            print(f"\rTempo restante: {i:3d} segundos", end="", flush=True)
            time.sleep(1)
        print("\n✅ Tempo de espera concluído!")

        # Acessar o BigQuery para obter o token
        print("Obtendo token do BigQuery...")
        try:
            from google.cloud import bigquery
            import json

            # Configurar cliente BigQuery usando as credenciais existentes
            credentials_path = GCSConfig.get_credentials_path()
            if credentials_path and os.path.exists(credentials_path):
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                client = bigquery.Client()

                # Query SQL para obter o token mais recente
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

                print("Executando query no BigQuery...")
                query_job = client.query(query)
                results = query_job.result()

                # Obter o token do resultado
                token_value = ""
                for row in results:
                    token_value = row.token
                    break

                if token_value:
                    print(
                        f"✅ Token obtido com sucesso do BigQuery: {token_value}")
                else:
                    print("❌ Nenhum token encontrado no BigQuery")
                    token_value = ""

            else:
                print("❌ Arquivo de credenciais do Google Cloud não encontrado")
                token_value = ""

        except Exception as e:
            print(f"❌ Erro ao acessar BigQuery: {e}")
            token_value = ""

        # Espera pelos 6 inputs de código (type='tel')
        print("Aguardando campos de código de verificação...")
        for _ in range(90):
            code_inputs = driver.find_elements(
                By.CSS_SELECTOR, "input[type='tel']")
            if len(code_inputs) == 6:
                print("Campos de código encontrados!")
                break
            time.sleep(1)
        else:
            print("❌ Campos de código de verificação não encontrados.")
            input("Pressione ENTER para fechar o navegador...")
            driver.quit()
            return
        code_digits = [c for c in token_value if c.isdigit()]
        for i, input_elem in enumerate(code_inputs):
            input_elem.clear()
            input_elem.send_keys(code_digits[i])
        print("Código de verificação preenchido nos campos.")
        try:
            continuar_btn = driver.find_element(
                By.CSS_SELECTOR, "button.btn.btn-success.btn-block")
            continuar_btn.click()
            print("Botão 'Continuar' clicado.")
        except Exception:
            print(
                "⚠️ Não foi possível clicar no botão 'Continuar'. Verifique manualmente.")
        print("Login no Clint finalizado!")
        time.sleep(3)

        # --- INÍCIO DO FLUXO DE DOWNLOAD ---
        nomes_arquivos = ["closer", "sdr_trab_barber",
                          "sdr_trab_business", "downsell_vendas_gerais"]
        bronze_data_path = os.path.join("data", "bronze", "data")
        bronze_bkp_path = os.path.join("data", "bronze", "bkp")
        os.makedirs(bronze_data_path, exist_ok=True)
        os.makedirs(bronze_bkp_path, exist_ok=True)

        # Remover arquivos existentes para sobrescrever
        print("Removendo arquivos existentes na pasta bronze/data...")
        for nome_arquivo in nomes_arquivos:
            arquivo_existente = os.path.join(
                bronze_data_path, f"{nome_arquivo}.csv")
            if os.path.exists(arquivo_existente):
                os.remove(arquivo_existente)
                print(f"Arquivo existente removido: {arquivo_existente}")

        for i, url in enumerate(ClintConfig.URLS_CLINT, 1):
            nome_arquivo = nomes_arquivos[i-1]
            print(f"\n{'='*50}")
            print(
                f"COLETANDO DADOS - URL {i} de {len(ClintConfig.URLS_CLINT)}")
            print(f"URL: {url}")
            print(f"{'='*50}")
            driver.get(url)
            time.sleep(5)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            print("Página carregada completamente.")
            current_url = driver.current_url
            print(f"URL atual: {current_url}")
            if url not in current_url:
                print(f"⚠️ Aviso: URL atual não corresponde à URL esperada")
                print(f"Esperado: {url}")
                print(f"Atual: {current_url}")
            time.sleep(3)
            # Filtro de status
            try:
                print("Procurando filtro de status...")
                status_filter = wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, '[data-cy="tbl-status-deal-filter"]')))
                status_filter.click()
                time.sleep(3)  # Aguarda o menu abrir completamente
                print("Filtro de status aberto.")

                # Selecionar apenas os status que não estão marcados por padrão
                status_list = ["Ganho", "Perdido"]
                for status in status_list:
                    try:
                        print(f"Procurando checkbox para status: {status}")
                        time.sleep(1)  # Pausa entre tentativas

                        # Tentar clicar diretamente no checkbox usando JavaScript
                        try:
                            # Seletor mais simples para encontrar o checkbox
                            checkbox_xpath = f"//input[@type='checkbox' and following-sibling::*[contains(text(), '{status}')]]"
                            checkbox = driver.find_element(
                                By.XPATH, checkbox_xpath)

                            if not checkbox.is_selected():
                                # Usar JavaScript para clicar se o clique normal falhar
                                driver.execute_script(
                                    "arguments[0].click();", checkbox)
                                print(
                                    f"Status '{status}' marcado via JavaScript.")
                            else:
                                print(f"Status '{status}' já estava marcado.")
                        except Exception as js_error:
                            print(
                                f"Tentativa JavaScript falhou para '{status}': {js_error}")
                            # Tentar método alternativo
                            try:
                                # Procurar por qualquer elemento que contenha o texto e clicar
                                element = driver.find_element(
                                    By.XPATH, f"//*[contains(text(), '{status}')]")
                                driver.execute_script(
                                    "arguments[0].click();", element)
                                print(
                                    f"Status '{status}' marcado via clique no texto.")
                            except Exception as alt_error:
                                print(
                                    f"Método alternativo também falhou para '{status}': {alt_error}")

                        time.sleep(1)
                    except Exception as e:
                        print(
                            f"⚠️ Não foi possível marcar o status '{status}': {e}")
            except Exception as e:
                print(f"❌ Erro ao abrir filtro de status: {e}")
                try:
                    driver.find_element(
                        By.CSS_SELECTOR, '[data-cy="tbl-status-deal-filter"]').click()
                    time.sleep(1)
                except:
                    pass
            # Download
            try:
                print("Procurando botão de download...")
                download_btn = wait.until(EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, '[data-cy="bt-export-deals-via-pipe"]')))
                download_btn.click()
                print("Botão de download clicado com sucesso!")
                time.sleep(2)
                # --- Tratamento de pop-up/modal de confirmação de download ---
                try:
                    # Clicar no botão 'Sim' do pop-up de confirmação
                    sim_btn = wait.until(EC.element_to_be_clickable(
                        (By.XPATH, "//button[normalize-space(text())='Sim']")))
                    sim_btn.click()
                    print("Botão 'Sim' do pop-up de confirmação clicado!")
                    time.sleep(2)
                except Exception as e:
                    print(
                        f"⚠️ Erro ao tentar clicar no botão 'Sim' do pop-up: {e}")
                # --- Fim do tratamento do pop-up ---
                time.sleep(5)
            except Exception as e:
                print(f"❌ Não foi possível clicar no botão de download: {e}")
            # Processar arquivo CSV
            try:
                download_folder = ClintConfig.get_downloads_path()
                csv_files = [f for f in os.listdir(
                    download_folder) if f.endswith(".csv")]
                if csv_files:
                    csv_files.sort(key=lambda x: os.path.getmtime(
                        os.path.join(download_folder, x)), reverse=True)
                    csv_path = os.path.join(download_folder, csv_files[0])
                    df = pd.read_csv(csv_path)
                    print(f"Arquivo CSV lido em DataFrame (URL {i}):")
                    print(df.head())
                    # Nome final do arquivo
                    nome_final = f"{nome_arquivo}.csv"
                    bronze_path = os.path.join(bronze_data_path, nome_final)
                    # Mover o arquivo baixado para bronze/data com o nome correto
                    os.replace(csv_path, bronze_path)
                    print(f"✅ Dados da URL {i} movidos para: {bronze_path}")
                else:
                    print("❌ Nenhum arquivo CSV encontrado na pasta Downloads.")
            except Exception as e:
                print(f"❌ Erro ao processar arquivo CSV da URL {i}: {e}")
            if i < len(ClintConfig.URLS_CLINT):
                print(f"\nAguardando 20 segundos antes da próxima coleta...")
                time.sleep(20)
            else:
                print(
                    f"\n✅ Coleta de todas as {len(ClintConfig.URLS_CLINT)} URLs finalizada!")
        # input("Pressione ENTER para fechar o navegador...")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
