@echo off
title P95 - ACCELERA360 - CLINT
setlocal enabledelayedexpansion

REM Definir variáveis de ambiente para o projeto Clint
set PROJECT_DIR=C:\Repositorio\Python\p95-accelera360
set LOG_FILE=%PROJECT_DIR%\logs\etl_execution.log

REM Criar pasta de logs se não existir
if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"

REM Registrar início da execução
echo [%date% %time%] Iniciando pipeline ETL Clint >> "%LOG_FILE%"

echo ========================================
echo    PIPELINE ETL CLINT - ACCELERA360
echo ========================================
echo.
echo Iniciando pipeline ETL completo...
echo Diretorio do projeto: %PROJECT_DIR%
echo.
echo Etapas do pipeline:
echo 1. Bronze Layer - RPA Clint (Web Scraping)
echo 2. Silver Layer - Processamento e Limpeza
echo 3. Gold Layer - Modelagem Dimensional + GCS
echo.

REM Verificar se o diretório do projeto existe
if not exist "%PROJECT_DIR%" (
    echo ERRO: Diretorio do projeto nao encontrado: %PROJECT_DIR%
    echo [%date% %time%] ERRO: Diretorio do projeto nao encontrado >> "%LOG_FILE%"
    goto :error
)

REM Mudar para o diretório do projeto
cd /d "%PROJECT_DIR%"
if errorlevel 1 (
    echo ERRO: Nao foi possivel mudar para o diretorio do projeto
    echo [%date% %time%] ERRO: Nao foi possivel mudar para o diretorio do projeto >> "%LOG_FILE%"
    goto :error
)

echo ========================================
echo    ETAPA 1: BRONZE LAYER (RPA)
echo ========================================
echo Executando RPA para capturar leads do Clint...
echo [%date% %time%] Iniciando Bronze Layer >> "%LOG_FILE%"

python scr\core\bronze\clint_main_modular.py todos
set BRONZE_EXIT_CODE=%errorlevel%

if %BRONZE_EXIT_CODE% neq 0 (
    echo ERRO: Falha na execucao do Bronze Layer
    echo [%date% %time%] ERRO: Falha na execucao do Bronze Layer >> "%LOG_FILE%"
    goto :error
)

echo ✅ Bronze Layer executado com sucesso!
echo [%date% %time%] Bronze Layer executado com sucesso >> "%LOG_FILE%"
echo.

echo ========================================
echo    ETAPA 2: SILVER LAYER
echo ========================================
echo Processando dados da camada Silver...
echo [%date% %time%] Iniciando Silver Layer >> "%LOG_FILE%"

python scr\core\silver\leads-forms-accelera.py
set SILVER_EXIT_CODE=%errorlevel%

if %SILVER_EXIT_CODE% neq 0 (
    echo ERRO: Falha na execucao do Silver Layer
    echo [%date% %time%] ERRO: Falha na execucao do Silver Layer >> "%LOG_FILE%"
    goto :error
)

echo ✅ Silver Layer executado com sucesso!
echo [%date% %time%] Silver Layer executado com sucesso >> "%LOG_FILE%"
echo.

echo ========================================
echo    ETAPA 3: GOLD LAYER + GCS
echo ========================================
echo Criando modelo dimensional e fazendo upload para GCS...
echo [%date% %time%] Iniciando Gold Layer >> "%LOG_FILE%"

python scr\core\gold\gold_clint_digital.py
set GOLD_EXIT_CODE=%errorlevel%

if %GOLD_EXIT_CODE% neq 0 (
    echo ERRO: Falha na execucao do Gold Layer
    echo [%date% %time%] ERRO: Falha na execucao do Gold Layer >> "%LOG_FILE%"
    goto :error
)

echo ✅ Gold Layer executado com sucesso!
echo [%date% %time%] Gold Layer executado com sucesso >> "%LOG_FILE%"
echo.

REM Verificar se todos os arquivos foram criados
echo ========================================
echo    VERIFICACAO DOS ARQUIVOS
echo ========================================
echo Verificando arquivos gerados...

if exist "data\bronze\leads-forms-accelera\leads-forms-accelera.csv" (
    echo ✅ Bronze: leads-forms-accelera.csv
) else (
    echo ❌ Bronze: leads-forms-accelera.csv nao encontrado
)

if exist "data\silver\leads-forms-accelera.parquet" (
    echo ✅ Silver: leads-forms-accelera.parquet
) else (
    echo ❌ Silver: leads-forms-accelera.parquet nao encontrado
)

if exist "data\gold\dim_cliente.parquet" (
    echo ✅ Gold: dim_cliente.parquet
) else (
    echo ❌ Gold: dim_cliente.parquet nao encontrado
)

if exist "data\gold\dim_vendedores.parquet" (
    echo ✅ Gold: dim_vendedores.parquet
) else (
    echo ❌ Gold: dim_vendedores.parquet nao encontrado
)

if exist "data\gold\dim_pipeline.parquet" (
    echo ✅ Gold: dim_pipeline.parquet
) else (
    echo ❌ Gold: dim_pipeline.parquet nao encontrado
)

if exist "data\gold\dim_estagio.parquet" (
    echo ✅ Gold: dim_estagio.parquet
) else (
    echo ❌ Gold: dim_estagio.parquet nao encontrado
)

if exist "data\gold\fato_clint_digital.parquet" (
    echo ✅ Gold: fato_clint_digital.parquet
) else (
    echo ❌ Gold: fato_clint_digital.parquet nao encontrado
)

echo.
echo ========================================
echo    PIPELINE ETL FINALIZADO COM SUCESSO
echo ========================================
echo.
echo ✅ Todas as etapas foram executadas com sucesso!
echo ✅ Dados processados e enviados para GCS
echo ✅ Arquivos locais gerados em data\
echo.
echo [%date% %time%] Pipeline ETL finalizado com sucesso >> "%LOG_FILE%"
echo [%date% %time%] Todas as etapas executadas: Bronze, Silver, Gold + GCS >> "%LOG_FILE%"

pause
exit /b 0

:error
echo.
echo ========================================
echo    ERRO NA EXECUCAO DO PIPELINE
echo ========================================
echo.
echo ❌ Ocorreu um erro durante a execucao
echo Verifique os logs em: %LOG_FILE%
echo.
echo [%date% %time%] Pipeline ETL finalizado com erro >> "%LOG_FILE%"
pause
exit /b 1 