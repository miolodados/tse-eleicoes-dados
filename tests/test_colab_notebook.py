"""Testes de integridade do notebook autocontido para Google Colab."""

import json
from pathlib import Path


def test_colab_notebook_structure():
    repo_root = Path(__file__).resolve().parent.parent
    nb_dir = repo_root / "notebooks" / "03_ingest_urna_logs_batch"
    nb_path = nb_dir / "colab_ingest_urna_logs_full.ipynb"
    assert nb_path.exists(), f"Notebook não encontrado em {nb_path}"

    with open(nb_path, encoding="utf-8") as f:
        data = json.load(f)

    assert data.get("nbformat") == 4
    cells = data.get("cells", [])
    assert len(cells) >= 8, f"Esperado ao menos 8 células, obteve {len(cells)}"

    # Verifica integridade das células markdown
    for idx, cell in enumerate(cells):
        if cell["cell_type"] == "markdown":
            assert "outputs" not in cell, f"Célula markdown {idx} não pode conter 'outputs'"
            assert "execution_count" not in cell, (
                f"Célula markdown {idx} não pode ter 'execution_count'"
            )

    full_text = "\n".join("".join(c["source"]) for c in cells)

    # Verifica palavras-chave críticas
    assert "KaggleApi" in full_text
    assert "dataset_create_version" in full_text
    assert "dataset_download_files" in full_text
    assert "dataset_metadata" in full_text
    assert "dir_mode" in full_text
    assert "tar" in full_text
    assert "userdata.get" in full_text
    assert "<ID_REDACTED>" in full_text
    assert "FACT_URNA_LOGS_ARROW_SCHEMA" in full_text
