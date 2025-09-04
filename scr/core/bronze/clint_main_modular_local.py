"""
Main Modular para RPA da Plataforma Clint
Orquestra todos os módulos mantendo a sessão compartilhada
"""

from clint_data_collection import executar_coleta_completa
from clint_token_verification import executar_verificacao_token
from clint_login import executar_login_completo
import time
import sys
import os

# Adicionar o diretório utils ao path para importação
utils_path = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), '..', '..', 'utils')
sys.path.append(utils_path)

# Adicionar o diretório atual ao path para importação dos módulos
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Importar módulos (APÓS configurar o path)


def executar_rpa_completo():
    """
    Executa o RPA completo em sequência, mantendo a sessão compartilhada
    """
    driver = None
    sucesso_total = True

    try:
        print("🚀 INICIANDO RPA COMPLETO DA PLATAFORMA CLINT")
        print("=" * 60)

        # ETAPA 1: LOGIN
        print("\n📋 ETAPA 1: LOGIN NA PLATAFORMA")
        print("-" * 40)
        driver = executar_login_completo()

        if not driver:
            print("❌ Falha na etapa de login. RPA interrompido.")
            return False

        print("✅ ETAPA 1 CONCLUÍDA: Login realizado com sucesso!")

        # ETAPA 2: VERIFICAÇÃO DE TOKEN
        print("\n📋 ETAPA 2: VERIFICAÇÃO DE TOKEN")
        print("-" * 40)
        driver = executar_verificacao_token(driver)

        if not driver:
            print("❌ Falha na etapa de verificação de token. RPA interrompido.")
            return False

        print("✅ ETAPA 2 CONCLUÍDA: Token verificado com sucesso!")

        # ETAPA 3: COLETA DE DADOS
        print("\n📋 ETAPA 3: COLETA DE DADOS")
        print("-" * 40)
        if not executar_coleta_completa(driver):
            print("⚠️ Falha na etapa de coleta de dados.")
            sucesso_total = False
        else:
            print("✅ ETAPA 3 CONCLUÍDA: Dados coletados com sucesso!")

        # RESULTADO FINAL
        print("\n" + "=" * 60)
        if sucesso_total:
            print("🎉 RPA COMPLETO EXECUTADO COM SUCESSO!")
            print("✅ Todas as etapas foram concluídas")
        else:
            print("⚠️ RPA EXECUTADO COM AVISOS")
            print("✅ Login e verificação concluídos")
            print("⚠️ Alguns problemas na coleta de dados")

        return sucesso_total

    except Exception as e:
        print(f"\n❌ ERRO CRÍTICO NO RPA: {e}")
        return False

    finally:
        # Sempre fechar o navegador
        if driver:
            print("\n🚪 Fechando navegador...")
            try:
                driver.quit()
                print("✅ Navegador fechado com sucesso!")
            except Exception as e:
                print(f"⚠️ Erro ao fechar navegador: {e}")


def executar_etapa_especifica(etapa):
    """
    Executa uma etapa específica do RPA

    Args:
        etapa (str): Nome da etapa ('login', 'token', 'dados', 'todos')
    """
    if etapa.lower() == 'login':
        print("🔐 Executando apenas etapa de LOGIN...")
        driver = executar_login_completo()
        if driver:
            print("✅ Login concluído! Driver retornado.")
            input("Pressione ENTER para fechar o navegador...")
            driver.quit()
        else:
            print("❌ Login falhou!")

    elif etapa.lower() == 'token':
        print("⚠️ Etapa de TOKEN requer driver já logado")
        print("💡 Execute primeiro: python clint_main_modular.py login")

    elif etapa.lower() == 'dados':
        print("⚠️ Etapa de DADOS requer driver já autenticado")
        print("💡 Execute primeiro: python clint_main_modular.py login")

    elif etapa.lower() == 'todos':
        executar_rpa_completo()

    else:
        print("❌ Etapa inválida. Use: login, token, dados ou todos")


def mostrar_menu():
    """
    Mostra menu de opções para o usuário
    """
    print("\n" + "=" * 60)
    print("🎯 RPA PLATAFORMA CLINT - MENU DE OPÇÕES")
    print("=" * 60)
    print("1. Executar RPA completo")
    print("2. Apenas login")
    print("3. Apenas verificação de token (requer login)")
    print("4. Apenas coleta de dados (requer autenticação)")
    print("5. Sair")
    print("-" * 60)

    try:
        opcao = input("Escolha uma opção (1-5): ").strip()

        if opcao == "1":
            executar_rpa_completo()
        elif opcao == "2":
            executar_etapa_especifica('login')
        elif opcao == "3":
            executar_etapa_especifica('token')
        elif opcao == "4":
            executar_etapa_especifica('dados')
        elif opcao == "5":
            print("👋 Saindo do RPA...")
            return False
        else:
            print("❌ Opção inválida!")

        return True

    except KeyboardInterrupt:
        print("\n\n👋 RPA interrompido pelo usuário")
        return False
    except Exception as e:
        print(f"\n❌ Erro no menu: {e}")
        return True


def main():
    """
    Função principal que verifica argumentos de linha de comando
    """
    if len(sys.argv) > 1:
        # Execução via linha de comando
        etapa = sys.argv[1]
        executar_etapa_especifica(etapa)
    else:
        # Execução interativa
        print("🚀 RPA PLATAFORMA CLINT - VERSÃO MODULAR")
        print("📋 Módulos: Login | Token | Coleta de Dados")
        print("🔗 Sessão compartilhada entre módulos")

        continuar = True
        while continuar:
            continuar = mostrar_menu()


if __name__ == "__main__":
    main()
