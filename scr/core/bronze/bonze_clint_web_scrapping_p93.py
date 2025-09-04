from config import ClintConfig
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import os
import pandas as pd
import sys

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
        time.sleep(5)

        # Abre nova aba para webmail
        print("Abrindo nova guia para o webmail...")
        driver.execute_script("window.open('about:blank','_blank');")
        time.sleep(1)
        webmail_handle = driver.window_handles[-1]
        clint_handle = driver.window_handles[0]
        driver.switch_to.window(webmail_handle)
        driver.get(ClintConfig.WEBMAIL_URL)
        print("Aba 2: Webmail aberta.")
        time.sleep(2)
        driver.find_element(By.ID, "rcmloginuser").send_keys(ClintConfig.EMAIL)
        driver.find_element(By.ID, "rcmloginpwd").send_keys(
            ClintConfig.PASSWORD)
        driver.find_element(By.ID, "rcmloginsubmit").click()
        print("Login no webmail enviado.")
        time.sleep(5)

        # Busca e coleta do código
        code = None
        rows = driver.find_elements(By.CSS_SELECTOR, "tr")
        for row in rows:
            try:
                remetente_elem = row.find_element(
                    By.CSS_SELECTOR, ".fromto .rcmContactAddress")
                remetente_title = remetente_elem.get_attribute("title")
                remetente_text = remetente_elem.text.strip()
                if (remetente_title and "oi@clint.digital" in remetente_title) or ("oi@clint.digital" in remetente_text):
                    row.click()
                    time.sleep(2)
                    iframes = driver.find_elements(By.TAG_NAME, "iframe")
                    if iframes:
                        driver.switch_to.frame(iframes[0])
                    try:
                        code_elem = driver.find_element(
                            By.CSS_SELECTOR, ".v1fa-email-content p b")
                        code = code_elem.text.strip()
                    except Exception:
                        body_text = driver.find_element(
                            By.TAG_NAME, "body").text
                        match = re.search(r"\\b\\d{3}-\\d{3}\\b", body_text)
                        if match:
                            code = match.group(0)
                    if iframes:
                        driver.switch_to.default_content()
                    break
            except Exception:
                continue
        if not code:
            print("❌ Não foi possível obter o código de verificação do e-mail.")
            input("Pressione ENTER para fechar o navegador...")
            driver.quit()
            return
        print(f"Código de verificação coletado: {code}")
        driver.close()
        driver.switch_to.window(clint_handle)
        print("Retornando para a aba do Clint.")

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
        code_digits = [c for c in code if c.isdigit()]
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
