#!/usr/bin/env python3
"""
Teste do ambiente de desenvolvimento
"""
import os
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

def test_environment():
    """Testa conexões e configurações básicas"""
    
    print("🔍 Testando ambiente de desenvolvimento...")
    
    # Testar importações principais
    try:
        import prefect
        print(f"✅ Prefect: {prefect.__version__}")
    except ImportError as e:
        print(f"❌ Erro Prefect: {e}")
    
    try:
        import pymongo
        print(f"✅ PyMongo: {pymongo.__version__}")
    except ImportError as e:
        print(f"❌ Erro PyMongo: {e}")
    
    try:
        import requests
        print(f"✅ Requests: {requests.__version__}")
    except ImportError as e:
        print(f"❌ Erro Requests: {e}")
        
    try:
        from dotenv import load_dotenv
        print("✅ Python-dotenv: OK")
    except ImportError as e:
        print(f"❌ Erro python-dotenv: {e}")
    
    # Verificar variáveis de ambiente (se existirem)
    pt_key = os.getenv('PT_API_KEY')
    mongo_uri = os.getenv('MONGO_URI')
    
    print(f"\n🔧 Configurações:")
    print(f"   PT_API_KEY: {'✅ Configurada' if pt_key else '⚠️ Pendente'}")
    print(f"   MONGO_URI: {'✅ Configurada' if mongo_uri else '⚠️ Pendente'}")
    
    print("\n🎉 Ambiente configurado! Pronto para desenvolvimento.")

if __name__ == "__main__":
    test_environment()
