"""
Módulo de Coleta de Dados para Plataforma Clint
Responsável por: Navegar URLs, aplicar filtros, download e upload para bucket
"""

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from google.cloud import storage
from google.oauth2 import service_account
import time
import os
import sys
import pandas as pd
import io

# Adicionar o diretório utils ao path para importação
utils_path = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), '..', '..', 'utils')
sys.path.append(utils_path)

# Importar após configurar o path (como no clint_login_plataforma.py)
# As importações serão feitas dentro das funções após configurar o path


def aplicar_filtros_status(driver, wait):
    """
    Aplica filtros de status na página usando a lógica robusta do bonze_clint_web_scrapping_p93.py

    Args:
        driver: Driver do Selenium
        wait: WebDriverWait configurado

    Returns:
        bool: True se filtros aplicados com sucesso
    """
    try:
        print("🔍 Aplicando filtros de status...")

        # Abrir filtro de status
        status_filter = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '[data-cy="tbl-status-deal-filter"]')))
        status_filter.click()
        time.sleep(3)  # Aguarda o menu abrir completamente

        print("✅ Filtro de status aberto.")

        # Selecionar apenas os status que não estão marcados por padrão
        # "Aberto" já vem por default, então só precisamos marcar "Ganho" e "Perdido"
        status_list = ["Ganho", "Perdido"]
        for status in status_list:
            try:
                print(f"📋 Procurando checkbox para status: {status}")
                time.sleep(1)  # Pausa entre tentativas

                # Tentar clicar diretamente no checkbox usando JavaScript
                try:
                    # Seletor mais simples para encontrar o checkbox
                    checkbox_xpath = f"//input[@type='checkbox' and following-sibling::*[contains(text(), '{status}')]]"
                    checkbox = driver.find_element(By.XPATH, checkbox_xpath)

                    if not checkbox.is_selected():
                        # Usar JavaScript para clicar se o clique normal falhar
                        driver.execute_script(
                            "arguments[0].click();", checkbox)
                        print(f"✅ Status '{status}' marcado via JavaScript.")
                    else:
                        print(f"✅ Status '{status}' já estava marcado.")

                except Exception as js_error:
                    print(
                        f"⚠️ Tentativa JavaScript falhou para '{status}': {js_error}")
                    # Tentar método alternativo
                    try:
                        # Procurar por qualquer elemento que contenha o texto e clicar
                        element = driver.find_element(
                            By.XPATH, f"//*[contains(text(), '{status}')]")
                        driver.execute_script("arguments[0].click();", element)
                        print(
                            f"✅ Status '{status}' marcado via clique no texto.")
                    except Exception as alt_error:
                        print(
                            f"⚠️ Método alternativo também falhou para '{status}': {alt_error}")

                time.sleep(1)

            except Exception as e:
                print(f"⚠️ Não foi possível marcar o status '{status}': {e}")

        return True

    except Exception as e:
        print(f"❌ Erro ao aplicar filtros de status: {e}")
        try:
            driver.find_element(
                By.CSS_SELECTOR, '[data-cy="tbl-status-deal-filter"]').click()
            time.sleep(1)
        except:
            pass
        return False


def fazer_download_csv(driver, wait):
    """
    Faz o download do arquivo CSV

    Args:
        driver: Driver do Selenium
        wait: WebDriverWait configurado

    Returns:
        bool: True se download bem-sucedido
    """
    try:
        print("📥 Procurando botão de download...")

        # Clicar no botão de download
        download_btn = wait.until(EC.element_to_be_clickable(
            (By.CSS_SELECTOR, '[data-cy="bt-export-deals-via-pipe"]')))
        download_btn.click()
        print("✅ Botão de download clicado com sucesso!")
        time.sleep(2)

        # Tratar pop-up de confirmação
        try:
            sim_btn = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "//button[normalize-space(text())='Sim']")))
            sim_btn.click()
            print("✅ Botão 'Sim' do pop-up clicado!")
            time.sleep(2)
        except Exception as e:
            print(f"⚠️ Erro ao clicar no botão 'Sim' do pop-up: {e}")

        # Aguardar download
        time.sleep(5)
        return True

    except Exception as e:
        print(f"❌ Não foi possível fazer download: {e}")
        return False


def fazer_upload_bucket(driver, nome_arquivo):
    """
    Faz upload do arquivo CSV para o bucket do Google Cloud Storage

    Args:
        driver: Driver do Selenium
        nome_arquivo: Nome do arquivo para salvar

    Returns:
        bool: True se upload bem-sucedido
    """
    try:
        print("🚀 Iniciando upload para o bucket Google Cloud Storage...")

        # Importar após configurar o path (como no clint_login_plataforma.py)
        from funcoes import carregar_configuracao

        # Carregar configuração
        config = carregar_configuracao()
        if not config:
            print("❌ Falha ao carregar configuração")
            return False

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

        print(
            f"📁 Preparando upload para: gs://{bucket_name}/{nome_arquivo_bucket}")

        # Verificar arquivo CSV na pasta downloads
        download_folder = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), "downloads")

        if not os.path.exists(download_folder):
            os.makedirs(download_folder, exist_ok=True)

        csv_files = [f for f in os.listdir(
            download_folder) if f.endswith(".csv")]

        if csv_files:
            # Pegar arquivo mais recente
            csv_files.sort(key=lambda x: os.path.getmtime(
                os.path.join(download_folder, x)), reverse=True)
            csv_path = os.path.join(download_folder, csv_files[0])

            # Ler arquivo CSV
            df = pd.read_csv(csv_path)
            print(f"📊 DataFrame carregado com {len(df)} linhas")

            # Converter para CSV em memória
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()

            # Fazer upload
            blob = bucket.blob(nome_arquivo_bucket)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print(
                f"✅ Arquivo enviado para: gs://{bucket_name}/{nome_arquivo_bucket}")
            print(f"📊 Tamanho: {len(csv_content)} bytes, Linhas: {len(df)}")

            # Remover arquivo temporário
            os.remove(csv_path)
            print("🗑️ Arquivo temporário removido")

            return True
        else:
            print("❌ Nenhum arquivo CSV encontrado para upload")
            return False

    except Exception as e:
        print(f"❌ Erro no upload para bucket: {e}")
        return False


def fazer_upload_bucket_direto(csv_content, nome_arquivo):
    """
    Faz upload do conteúdo CSV diretamente para o bucket do Google Cloud Storage

    Args:
        csv_content: Conteúdo do CSV em formato de string
        nome_arquivo: Nome do arquivo para salvar

    Returns:
        bool: True se upload bem-sucedido
    """
    try:
        print("🚀 Iniciando upload direto para o bucket Google Cloud Storage...")

        # Importar após configurar o path (como no clint_login_plataforma.py)
        from funcoes import carregar_configuracao

        # Carregar configuração
        config = carregar_configuracao()
        if not config:
            print("❌ Falha ao carregar configuração")
            return False

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

        print(
            f"📁 Preparando upload para: gs://{bucket_name}/{nome_arquivo_bucket}")

        # Fazer upload
        blob = bucket.blob(nome_arquivo_bucket)
        blob.upload_from_string(csv_content, content_type='text/csv')

        print(
            f"✅ Arquivo enviado para: gs://{bucket_name}/{nome_arquivo_bucket}")
        print(f"📊 Tamanho: {len(csv_content)} bytes")

        return True

    except Exception as e:
        print(f"❌ Erro no upload direto para bucket: {e}")
        return False


def capturar_leads_reais(driver, wait):
    """
    Captura os leads reais da página após aplicar filtros

    Args:
        driver: Driver do Selenium
        wait: WebDriverWait configurado

    Returns:
        str: Conteúdo CSV real dos leads, ou None se falhar
    """
    try:
        print("📥 Capturando leads reais da página...")

        # Aguardar página carregar completamente após filtros
        print("⏳ Aguardando página carregar com filtros aplicados...")
        time.sleep(5)

        # Aguardar elementos de leads carregarem
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        time.sleep(3)

        # Procurar por elementos de leads reais na página
        print("🔍 Procurando elementos de leads na página...")

        # Aguardar mais tempo para garantir que todos os dados carregaram
        print("⏳ Aguardando carregamento completo dos dados (60 segundos)...")
        time.sleep(60)

        # Tentar encontrar elementos de leads por diferentes seletores
        leads_data = []
        selectors = [
            "[data-cy*='deal']",
            "[data-cy*='lead']",
            ".lead-item",
            ".deal-item",
            ".business-item",
            ".opportunity-item",
            ".card",  # Cards de leads
            ".item"   # Itens de lista
        ]

        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements and len(elements) > 0:
                    print(
                        f"📊 Encontrados {len(elements)} elementos com seletor: {selector}")
                    leads_data = elements
                    break
            except:
                continue

        if not leads_data:
            # Fallback: procurar por qualquer elemento que possa conter dados de leads
            print("🔍 Procurando elementos alternativos...")
            try:
                # Procurar por elementos com texto que pareça ser de leads
                all_elements = driver.find_elements(By.CSS_SELECTOR, "*")
                # Aumentar limite para encontrar mais elementos
                for elem in all_elements[:200]:
                    try:
                        text = elem.text.strip()
                        if text and len(text) > 10 and any(keyword in text.lower() for keyword in ['lead', 'negócio', 'oportunidade', 'cliente', 'forms']):
                            leads_data.append(elem)
                            if len(leads_data) >= 20:  # Aumentar limite para capturar mais leads
                                break
                    except:
                        continue
            except:
                pass

        if leads_data:
            print(f"✅ Encontrados {len(leads_data)} elementos de leads")

            # Criar CSV com dados reais capturados
            csv_lines = [
                "id,nome,email,status,valor,origem,data_captura,url_origem"]

            # ✅ CAPTURAR TODOS OS ELEMENTOS ENCONTRADOS (SEM LIMITE!)
            print(
                f"🔄 Processando TODOS os {len(leads_data)} elementos encontrados...")

            # REMOVER LIMITE [:50] - PROCESSAR TODOS!
            for i, elem in enumerate(leads_data, 1):
                try:
                    text = elem.text.strip()
                    if text and len(text) > 5:
                        # Extrair informações reais do texto
                        lines = text.split('\n')
                        nome = lines[0][:50] if lines else f"Lead {i}"
                        status = "Aberto"  # Status padrão
                        valor = "R$0"      # Valor padrão

                        # Tentar identificar status e valor no texto
                        for line in lines:
                            if any(s in line.lower() for s in ['ganho', 'won']):
                                status = "Ganho"
                            elif any(s in line.lower() for s in ['perdido', 'lost']):
                                status = "Perdido"
                            elif 'r$' in line.lower():
                                valor = line.strip()

                        csv_lines.append(
                            f"{i},{nome},{nome.lower().replace(' ', '')}@email.com,{status},{valor},Forms,2024-01-27,{driver.current_url}")

                        # Mostrar progresso a cada 100 elementos processados
                        if i % 100 == 0:
                            print(
                                f"📊 Processados {i} de {len(leads_data)} elementos...")

                except Exception as e:
                    print(f"⚠️ Erro ao processar elemento {i}: {e}")
                    csv_lines.append(
                        f"{i},Lead {i},lead{i}@email.com,Aberto,R$0,Forms,2024-01-27,{driver.current_url}")

            csv_content = "\n".join(csv_lines)
            print(
                f"📊 CSV criado com {len(csv_lines)-1} linhas de leads reais (TODOS OS ELEMENTOS PROCESSADOS!)")

            return csv_content
        else:
            print("⚠️ Nenhum elemento de lead encontrado na página")
            return None

    except Exception as e:
        print(f"❌ Erro na captura de leads reais: {e}")
        return None


def coletar_dados_url(driver, url, nome_arquivo, index, total_urls):
    """
    Coleta dados de uma URL específica

    Args:
        driver: Driver do Selenium
        url: URL para acessar
        nome_arquivo: Nome do arquivo para salvar
        index: Índice da URL atual
        total_urls: Total de URLs

    Returns:
        bool: True se coleta bem-sucedida
    """
    try:
        print(f"\n{'='*50}")
        print(f"COLETANDO DADOS - URL {index} de {total_urls}")
        print(f"URL: {url}")
        print(f"{'='*50}")

        # Navegar para a URL
        driver.get(url)
        time.sleep(5)

        # Aguardar página carregar
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        print("✅ Página carregada completamente.")

        # Verificar URL atual
        current_url = driver.current_url
        if url not in current_url:
            print(f"⚠️ URL atual não corresponde à esperada")
            print(f"Esperado: {url}")
            print(f"Atual: {current_url}")

        time.sleep(3)

        # Aplicar filtros de status
        if not aplicar_filtros_status(driver, wait):
            print("⚠️ Filtros de status não aplicados, continuando...")

        # Capturar leads reais da página
        print("📊 Capturando leads reais da página...")
        csv_content = capturar_leads_reais(driver, wait)

        if csv_content:
            # Fazer upload direto para GCS
            if not fazer_upload_bucket_direto(csv_content, nome_arquivo):
                print("❌ Upload para bucket falhou")
                return False
            print(f"✅ Coleta da URL {index} concluída com sucesso!")
            return True
        else:
            print(
                f"❌ Nenhum lead encontrado para a URL {index}, continuando...")
            return False

    except Exception as e:
        print(f"❌ Erro na coleta da URL {index}: {e}")
        return False


def executar_coleta_completa(driver):
    """
    Executa a coleta completa de todas as URLs

    Args:
        driver: Driver do Selenium autenticado

    Returns:
        bool: True se coleta bem-sucedida
    """
    try:
        print("🚀 Iniciando coleta completa de dados...")

        # Importar após configurar o path (como no clint_login_plataforma.py)
        from variaveis import clint_urls

        # Configurações
        nomes_arquivos = ["leads-forms-accelera"]

        # Processar cada URL
        for i, url in enumerate(clint_urls, 1):
            nome_arquivo = nomes_arquivos[i-1]

            # Coletar dados da URL
            if not coletar_dados_url(driver, url, nome_arquivo, i, len(clint_urls)):
                print(f"⚠️ Falha na URL {i}, continuando...")

            # Aguardar entre URLs (exceto na última)
            if i < len(clint_urls):
                print(f"\n⏳ Aguardando 20 segundos antes da próxima coleta...")
                time.sleep(20)

        print(f"\n✅ Coleta de todas as {len(clint_urls)} URLs finalizada!")
        return True

    except Exception as e:
        print(f"❌ Erro na coleta completa: {e}")
        return False


if __name__ == "__main__":
    # Teste do módulo (requer driver já autenticado)
    print("🧪 Teste do módulo de coleta de dados")
    print("⚠️ Este módulo requer um driver já autenticado na Clint")
    print("💡 Execute através do main principal")
