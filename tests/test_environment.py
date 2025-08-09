"""Testes básicos do ambiente de desenvolvimento."""

import importlib
import os

from dotenv import load_dotenv


load_dotenv()


def test_environment() -> None:
    """Verifica importação de pacotes e variáveis de ambiente."""

    for package in ["prefect", "pymongo", "requests", "dotenv"]:
        assert importlib.import_module(package) is not None

    assert os.getenv("PT_API_KEY") is not None
    assert os.getenv("MONGO_URI") is not None
