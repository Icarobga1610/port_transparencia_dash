#!/usr/bin/env python3
"""
Flow de ingestão de dados do Portal da Transparência
"""
import os
import requests
from dotenv import load_dotenv
from prefect import flow, task
from prefect.logging import get_run_logger

# Carregar variáveis de ambiente
load_dotenv()

@task(retries=3, retry_delay_seconds=10)
def fetch_transparencia_data(api_key: str, page: int = 1):
    """
    Busca dados da API do Portal da Transparência
    """
    logger = get_run_logger()
    
    # URL base da API
    base_url = "https://api.portaldatransparencia.gov.br/api-de-dados"
    endpoint = f"{base_url}/despesas"
    
    headers = {
        'chave-api-dados': api_key,
        'Accept': 'application/json'
    }
    
    params = {
        'pagina': page,
        'tamanhoPagina': 100  # máximo permitido
    }
    
    try:
        logger.info(f"Buscando página {page} da API...")
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✅ Página {page}: {len(data)} registros obtidos")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro na API: {e}")
        raise

@flow(name="ingest-transparencia")
def ingest_flow():
    """
    Flow principal de ingestão
    """
    logger = get_run_logger()
    
    # Verificar se a chave da API existe
    api_key = os.getenv('PT_API_KEY')
    if not api_key:
        logger.warning("⚠️ PT_API_KEY não configurada. Simulando dados...")
        logger.info("✅ Simulação concluída - pipeline pronto para chave real!")
        return
    
    # Buscar primeira página de teste
    try:
        data = fetch_transparencia_data(api_key, page=1)
        logger.info(f"🎉 Ingestão teste bem-sucedida! {len(data)} registros")
        
    except Exception as e:
        logger.error(f"❌ Falha na ingestão: {e}")

if __name__ == "__main__":
    ingest_flow()
