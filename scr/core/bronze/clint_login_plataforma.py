# from config import ClintConfig
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium import webdriver
import time
import re
import os
import pandas as pd
import sys

# Adicionar o diretório scripts ao path para importação
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
print("✅ Diretório scripts adicionado ao path")

# Carregar variáveis do arquivo variaveis.py
variaveis_path = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), '..', '..', 'utils', 'variaveis.py')

# Executar o arquivo variaveis.py para carregar as variáveis
with open(variaveis_path, 'r') as f:
    exec(f.read())

# Importações do Selenium (após configurar o path)

#### INÍCIO DO FLUXO DE WEBB SCRAPPING ####
def main():
    print("🚀 Iniciando RPA para acessar a plataforma Clint...")

    # Verificar se as variáveis de ambiente estão configuradas
    if not clint_url:
        print("❌ Erro: URL_CLINT não configurada no arquivo .env")
        return

    if not clint_user:
        print("❌ Erro: CLINT_USER não configurado no arquivo .env")
        return

    if not clint_password:
        print("❌ Erro: CLINT_PASSWORD não configurado no arquivo .env")
        return

    print(f"✅ URL da Clint: {clint_url}")
    print(f"✅ Usuário: {clint_user}")

    # Configurar Chrome para automação
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument(
        "--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option(
        "excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # Configurar pasta de downloads
    downloads_path = os.path.join(os.path.dirname(
        os.path.abspath(__file__)), "downloads")
    os.makedirs(downloads_path, exist_ok=True)

    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": os.path.abspath(downloads_path),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    #### INÍCIO DO FLUXO DE LOGIN ####
    try:
        print("🌐 Iniciando navegador Chrome...")
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        wait = WebDriverWait(driver, 30)

        # Acessar o site da Clint
        print(f"🔗 Acessando: {clint_url}")
        driver.get(clint_url)
        time.sleep(3)

        print("✅ Site da Clint carregado com sucesso!")
        print(f"📄 Título da página: {driver.title}")

        # Aguardar a página de login carregar
        print("⏳ Aguardando página de login carregar...")
        time.sleep(3)

        # Realizar login automático
        try:
            print("🔐 Iniciando processo de login...")

            # Aguardar o campo de email aparecer
            email_field = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "input[placeholder='Email']")))
            print("✅ Campo de email encontrado")

            # Preencher email
            email_field.clear()
            email_field.send_keys(clint_user)
            print(f"✅ Email preenchido: {clint_user}")
            time.sleep(1)

            # Aguardar o campo de senha aparecer
            password_field = wait.until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "input[placeholder='Senha']")))
            print("✅ Campo de senha encontrado")

            # Preencher senha
            password_field.clear()
            password_field.send_keys(clint_password)
            print("✅ Senha preenchida")
            time.sleep(1)

            # Clicar no botão de continuar
            continue_button = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[contains(text(), 'Continuar')]")))
            continue_button.click()
            print("✅ Botão de continuar clicado")

            # Aguardar um pouco para o login processar
            print("⏳ Aguardando processamento do login...")
            time.sleep(5)

            print("✅ Login realizado com sucesso!")

            # Importar e usar a função de contagem regressiva
            # Adicionar o diretório utils ao path para importação
            utils_path = os.path.join(os.path.dirname(
                os.path.abspath(__file__)), '..', '..', 'utils')
            sys.path.append(utils_path)

            #### INÍCIO DO FLUXO DE VERIFICAÇÃO DE CÓDIGO ####
            try:
                from funcoes import contagem_regressiva
                contagem_regressiva(60)

                from funcoes import capturar_codigo_acesso_client
                token_value = capturar_codigo_acesso_client()

                if not token_value:
                    print("❌ Falha ao capturar token do BigQuery")
                    input("Pressione ENTER para fechar o navegador...")
                    driver.quit()
                    return

                # Espera pelos 6 inputs de código (type='tel')
                print("🔍 Aguardando campos de código de verificação...")
                for _ in range(90):
                    code_inputs = driver.find_elements(
                        By.CSS_SELECTOR, "input[type='tel']")
                    if len(code_inputs) == 6:
                        print("✅ Campos de código encontrados!")
                        break
                    time.sleep(1)
                else:
                    print("❌ Campos de código de verificação não encontrados.")
                    input("Pressione ENTER para fechar o navegador...")
                    driver.quit()
                    return

                #### FIM DO FLUXO DE VERIFICAÇÃO DE CÓDIGO ####

                #### INÍCIO DO FLUXO DE PREENCHIMENTO DE CÓDIGO ####
                # Preencher o código de verificação
                code_digits = [c for c in token_value if c.isdigit()]
                if len(code_digits) >= 6:
                    for i, input_elem in enumerate(code_inputs):
                        input_elem.clear()
                        input_elem.send_keys(code_digits[i])
                    print("✅ Código de verificação preenchido nos campos.")

                    try:
                        continuar_btn = driver.find_element(
                            By.CSS_SELECTOR, "button.btn.btn-success.btn-block")
                        continuar_btn.click()
                        print("✅ Botão 'Continuar' clicado.")
                    except Exception as e:
                        print(
                            f"⚠️ Não foi possível clicar no botão 'Continuar': {e}")

                    print("🎉 Login no Clint finalizado com sucesso!")
                    time.sleep(3)
                else:
                    print(f"❌ Token inválido: {token_value}")

            except ImportError as e:
                print(f"⚠️ Erro de importação: {e}")
                print("🔄 Usando contador padrão...")
                # Contador regressivo de 90 segundos (fallback)
                countdown_time = 90
                print(f"Aguardando {countdown_time} segundos...")
                for i in range(countdown_time, 0, -1):
                    print(
                        f"\rTempo restante: {i:3d} segundos", end="", flush=True)
                    time.sleep(1)
                print("\n✅ Tempo de espera concluído!")

                #### FIM DO FLUXO DE PREENCHIMENTO DE CÓDIGO ####

                #### INÍCIO DO FLUXO DE DOWNLOAD ####
                nomes_arquivos = ["leads-forms-accelera"]
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

                for i, url in enumerate(clint_urls, 1):
                    nome_arquivo = nomes_arquivos[i-1]
                    print(f"\n{'='*50}")
                    print(
                        f"COLETANDO DADOS - URL {i} de {len(clint_urls)}")
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
                    #### FIM DO FLUXO DE FILTRO DE STATUS ####

                    #### INÍCIO DO FLUXO DE DOWNLOAD ####
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
                    #### FIM DO FLUXO DE DOWNLOAD ####

                    #### INÍCIO DO FLUXO DE PROCESSAMENTO DIRETO NO BUCKET ####
                    try:
                        print("🚀 Iniciando upload direto para o bucket Google Cloud Storage...")
                        
                        # Importar biblioteca do Google Cloud Storage
                        from google.cloud import storage
                        from google.oauth2 import service_account
                        import io
                        
                        # Carregar configuração para obter credenciais
                        config = carregar_configuracao()
                        if not config:
                            print("❌ Falha ao carregar configuração")
                            continue
                            
                        # Obter credenciais
                        credentials = service_account.Credentials.from_service_account_file(
                            config['credentials-path'],
                            scopes=["https://www.googleapis.com/auth/cloud-platform"]
                        )
                        
                        # Criar cliente do Storage
                        storage_client = storage.Client(credentials=credentials)
                        bucket_name = "p95-accelera360"
                        bucket = storage_client.bucket(bucket_name)
                        
                        # Nome do arquivo no bucket
                        nome_arquivo_bucket = f"bronze/leads-forms-accelera/{nome_arquivo}.csv"
                        
                        print(f"📁 Preparando upload para: gs://{bucket_name}/{nome_arquivo_bucket}")
                        
                        # Aguardar um pouco para garantir que o download foi processado
                        time.sleep(3)
                        
                        # Verificar se há arquivos CSV na pasta downloads (temporário)
                        download_folder = os.path.join(os.path.dirname(
                            os.path.abspath(__file__)), "downloads")
                        
                        if not os.path.exists(download_folder):
                            os.makedirs(download_folder, exist_ok=True)
                            
                        csv_files = [f for f in os.listdir(download_folder) if f.endswith(".csv")]
                        
                        if csv_files:
                            # Pegar o arquivo mais recente
                            csv_files.sort(key=lambda x: os.path.getmtime(
                                os.path.join(download_folder, x)), reverse=True)
                            csv_path = os.path.join(download_folder, csv_files[0])
                            
                            # Ler o arquivo CSV
                            df = pd.read_csv(csv_path)
                            print(f"📊 DataFrame carregado com {len(df)} linhas")
                            
                            # Converter DataFrame para CSV em memória
                            csv_buffer = io.StringIO()
                            df.to_csv(csv_buffer, index=False)
                            csv_content = csv_buffer.getvalue()
                            
                            # Fazer upload direto para o bucket
                            blob = bucket.blob(nome_arquivo_bucket)
                            blob.upload_from_string(csv_content, content_type='text/csv')
                            
                            print(f"✅ Arquivo enviado com sucesso para: gs://{bucket_name}/{nome_arquivo_bucket}")
                            print(f"📊 Tamanho do arquivo: {len(csv_content)} bytes")
                            print(f"📈 Linhas processadas: {len(df)}")
                            
                            # Remover arquivo temporário local
                            os.remove(csv_path)
                            print("🗑️ Arquivo temporário local removido")
                            
                        else:
                            print("❌ Nenhum arquivo CSV encontrado para upload")
                            
                    except Exception as e:
                        print(f"❌ Erro ao fazer upload para o bucket (URL {i}): {e}")
                        print("🔄 Tentando método alternativo...")
                        
                        try:
                            # Método alternativo: verificar se há dados na página
                            print("🔍 Verificando se há dados na página para captura direta...")
                            
                            # Aqui você pode implementar captura direta dos dados da página
                            # Por exemplo, capturar tabelas ou elementos específicos
                            
                            print("⚠️ Método alternativo não implementado ainda")
                            
                        except Exception as e2:
                            print(f"❌ Método alternativo também falhou: {e2}")
                    #### FIM DO FLUXO DE PROCESSAMENTO DIRETO NO BUCKET ####

                    if i < len(clint_urls):
                        print(f"\nAguardando 20 segundos antes da próxima coleta...")
                        time.sleep(20)
                    else:
                        print(
                            f"\n✅ Coleta de todas as {len(clint_urls)} URLs finalizada!")
                    #### FIM DO FLUXO DE PROCESSAMENTO DO ARQUIVO CSV ####

                #### FIM DO FLUXO DE DOWNLOAD ####

                #### FIM DO FLUXO DE WEBB ####

                # input("Pressione ENTER para fechar o navegador...")

        except Exception as e:
            print(f"❌ Erro durante o login: {e}")
            print("🔄 Tentando método alternativo de login...")

            try:
                # Método alternativo usando XPath mais específico
                email_field = driver.find_element(
                    By.XPATH, "//input[@placeholder='Email']")
                email_field.clear()
                email_field.send_keys(clint_user)
                print(f"✅ Email preenchido (método alternativo): {clint_user}")

                password_field = driver.find_element(
                    By.XPATH, "//input[@placeholder='Senha']")
                password_field.clear()
                password_field.send_keys(clint_password)
                print("✅ Senha preenchida (método alternativo)")

                continue_button = driver.find_element(
                    By.XPATH, "//button[text()='Continuar']")
                continue_button.click()
                print("✅ Botão de continuar clicado (método alternativo)")

                time.sleep(5)
                print("✅ Login realizado com sucesso (método alternativo)!")

            except Exception as e2:
                print(f"❌ Erro no método alternativo: {e2}")

        # Aguardar um pouco para visualizar
        print("⏳ Aguardando 5 segundos para visualização...")
        time.sleep(5)

    except Exception as e:
        print(f"❌ Erro durante a execução do RPA: {e}")
    finally:
        # Fechar o navegador
        if 'driver' in locals():
            driver.quit()
            print("✅ Navegador fechado.")


if __name__ == "__main__":
    main()
