"""Testes de validação dos metadados Kaggle em pt-BR."""

import json
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = REPO_ROOT / "configs" / "kaggle_metadata"
TABLES_DIR = CONFIG_DIR / "tabelas"
METADATA_JSON = CONFIG_DIR / "dataset-metadata.json"


def test_metadata_json_exists_and_valid():
    """Valida se o JSON compilado existe e possui sintaxe válida."""
    assert METADATA_JSON.exists(), "dataset-metadata.json deve existir"
    with open(METADATA_JSON, encoding="utf-8") as f:
        data = json.load(f)

    assert "title" in data
    assert "subtitle" in data
    assert "description" in data
    assert "licenses" in data
    assert "isPrivate" in data
    assert "expectedUpdateFrequency" in data


def test_kaggle_length_and_field_constraints():
    """Valida limites e regras rígidas da API do Kaggle."""
    with open(METADATA_JSON, encoding="utf-8") as f:
        data = json.load(f)

    title = data["title"]
    subtitle = data["subtitle"]

    assert 6 <= len(title) <= 50, f"Title length {len(title)} fora do limite [6, 50]"
    assert 20 <= len(subtitle) <= 80, f"Subtitle length {len(subtitle)} fora do limite [20, 80]"
    assert data["isPrivate"] is True, "Dataset deve ser privado conforme decisão do usuário"
    assert data["expectedUpdateFrequency"] == "never"
    assert any(lic.get("name") in ("CC-BY-4.0", "CC-BY-SA-4.0") for lic in data["licenses"])


def test_markdown_description_content():
    """Verifica se a descrição em Markdown contém todas as seções essenciais em pt-BR."""
    with open(METADATA_JSON, encoding="utf-8") as f:
        data = json.load(f)

    desc = data["description"]
    assert "Bronze" in desc
    assert "Silver" in desc
    assert "Gold" in desc
    assert "LGPD" in desc or "Privacidade" in desc
    assert "DuckDB" in desc
    assert "Polars" in desc
    assert "Tribunal Superior Eleitoral" in desc


def test_table_yamls_consistency():
    """Valida se todos os 25 arquivos de tabela possuem os campos obrigatórios."""
    yaml_files = list(TABLES_DIR.glob("*.yml"))
    assert len(yaml_files) >= 25, f"Esperado pelo menos 25 tabelas, encontrado {len(yaml_files)}"

    for yml in yaml_files:
        with open(yml, encoding="utf-8") as f:
            t = yaml.safe_load(f)
            assert "tabela" in t, f"{yml.name} sem chave 'tabela'"
            assert "camada" in t, f"{yml.name} sem chave 'camada'"
            valid_layers = ("bronze", "silver", "gold")
            assert t["camada"] in valid_layers, f"{yml.name} camada inválida: {t['camada']}"
            assert "descricao" in t, f"{yml.name} sem 'descricao'"
            assert "colunas" in t and len(t["colunas"]) > 0, f"{yml.name} sem colunas definidas"
