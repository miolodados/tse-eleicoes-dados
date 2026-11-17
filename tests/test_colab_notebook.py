"""Testes de contrato e bootstrap KaggleHub SDK em todos os 9 notebooks faseados."""

from __future__ import annotations

import ast
import json
import os
import re
import sys
import tempfile
import types
from pathlib import Path

PHASED_NOTEBOOKS = [
    ("notebooks/01_urna_logs_poc/01_urna_logs_poc.ipynb", True),
    ("notebooks/02_ingest_electoral_tables/02_ingest_electoral_tables.ipynb", True),
    ("notebooks/03_ingest_urna_logs_batch/03_ingest_urna_logs_batch.ipynb", True),
    ("notebooks/04_dataset_integrity_and_release/04_dataset_integrity_and_release.ipynb", False),
    ("notebooks/05_silver_fact_urna_event/05_silver_fact_urna_event.ipynb", False),
    ("notebooks/06_silver_fact_voting_session/06_silver_fact_voting_session.ipynb", False),
    ("notebooks/07_silver_dimensions/07_silver_dimensions.ipynb", False),
    ("notebooks/08_gold_analytics/08_gold_analytics.ipynb", False),
    ("notebooks/09_release_pipeline/09_release_pipeline.ipynb", False),
]

REPO_ROOT = Path(__file__).resolve().parent.parent


def _extract_bootstrap_namespace(nb_path: Path) -> dict[str, object]:
    """Executa apenas definições de bootstrap da célula inicial em namespace isolado."""
    nb_data = json.loads(nb_path.read_text(encoding="utf-8"))
    code_cells = [c for c in nb_data["cells"] if c.get("cell_type") == "code"]
    src = "".join(code_cells[0]["source"])
    tree = ast.parse(src)

    keep_names = {"DATASET_HANDLE", "DATASET_PATH", "STAGING_PATH", "ALLOW_COLD_START"}
    keep_funcs = {"is_directory_empty", "bootstrap_dataset"}
    keep_imports = {"os", "shutil", "pathlib"}

    selected_nodes: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.Import) and any(
            alias.name in keep_imports for alias in node.names
        ):
            selected_nodes.append(node)
        elif isinstance(node, ast.ImportFrom) and node.module in keep_imports:
            selected_nodes.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in keep_names for t in node.targets
        ):
            selected_nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in keep_funcs:
            selected_nodes.append(node)

    mod = ast.Module(body=selected_nodes, type_ignores=[])
    ast.fix_missing_locations(mod)
    ns: dict[str, object] = {}
    exec(compile(mod, str(nb_path), "exec"), ns)
    return ns


def test_parallel_colab_artifacts_removed() -> None:
    """Garante que o notebook paralelo de Colab e seu script gerador foram removidos."""
    assert not (
        REPO_ROOT / "notebooks" / "03_ingest_urna_logs_batch" / "colab_ingest_urna_logs_full.ipynb"
    ).exists()
    assert not (REPO_ROOT / "scripts" / "build_colab_notebook.py").exists()


def test_all_phased_notebooks_kagglehub_contract_and_compile() -> None:
    """Valida contrato KaggleHub SDK, ausência de caminhos legados e compilação nos 9 notebooks."""
    required_tokens = [
        "DATASET_HANDLE",
        "DATASET_PATH",
        "STAGING_PATH",
        "/data/export/tse-eleicoes-dados",
        "/tmp/tse-eleicoes-staging",
        "is_directory_empty",
        "bootstrap_dataset",
        "kagglehub.login()",
        "kagglehub.dataset_download",
        "output_dir",
        "force_download=True",
        "kagglehub.dataset_upload",
        "ignore_patterns",
        "forbidden_extensions",
    ]
    forbidden_legacy = [
        "/kaggle/working",
        "/kaggle/input",
        "/content/drive",
        "--dir-mode",
        "daniellmattos/tse-analysis-eleicoes-2026",
    ]

    for rel_path, expected_cold_start in PHASED_NOTEBOOKS:
        nb_path = REPO_ROOT / rel_path
        assert nb_path.exists(), f"Notebook faseado ausente: {nb_path}"

        raw_text = nb_path.read_text(encoding="utf-8")
        data = json.loads(raw_text)
        assert data.get("nbformat") == 4, f"{rel_path}: nbformat inválido"

        cells = data.get("cells", [])
        code_sources: list[str] = []
        for idx, cell in enumerate(cells):
            if cell["cell_type"] == "markdown":
                assert "outputs" not in cell, f"{rel_path} cell[{idx}] markdown com outputs"
                assert "execution_count" not in cell, (
                    f"{rel_path} cell[{idx}] markdown com execution_count"
                )
            elif cell["cell_type"] == "code":
                src = "".join(cell.get("source", []))
                compile(src, f"{rel_path}#cell{idx}", "exec")
                code_sources.append(src)

        full_code = "\n".join(code_sources)
        for token in required_tokens:
            assert token in full_code, f"{rel_path} sem token obrigatório: {token!r}"

        expected_cold_str = f"ALLOW_COLD_START = {expected_cold_start}"
        assert expected_cold_str in code_sources[0], (
            f"{rel_path} cell[0] deve definir {expected_cold_str!r}"
        )

        for legacy in forbidden_legacy:
            assert legacy not in raw_text, f"{rel_path} contém referência legada {legacy!r}"


def test_existing_notebook_test_invariants_preserved() -> None:
    """Preserva invariantes exigidos pelas suítes de testes dos notebooks 02, 03 e 04."""
    for rel in (
        "notebooks/02_ingest_electoral_tables/02_ingest_electoral_tables.ipynb",
        "notebooks/03_ingest_urna_logs_batch/03_ingest_urna_logs_batch.ipynb",
    ):
        nb_path = REPO_ROOT / rel
        raw = nb_path.read_text(encoding="utf-8")
        assert ("bronze" + "_") not in raw
        assert "Decisão A1" not in raw
        assert "Decisão A2" not in raw

        data = json.loads(raw)
        code_cells = [c for c in data["cells"] if c.get("cell_type") == "code"]
        assert len(code_cells) == 4
        for cell in code_cells:
            assert cell.get("execution_count") is not None

        stdout_text = "".join(
            "".join(out.get("text", []))
            for cell in code_cells
            for out in cell.get("outputs", [])
            if out.get("output_type") == "stream" and out.get("name") == "stdout"
        )
        assert re.search(r"SOMA TOTAL EM DISCO", stdout_text)

    nb04_path = (
        REPO_ROOT
        / "notebooks"
        / "04_dataset_integrity_and_release"
        / "04_dataset_integrity_and_release.ipynb"
    )
    nb04_data = json.loads(nb04_path.read_text(encoding="utf-8"))
    nb04_code = [c for c in nb04_data["cells"] if c.get("cell_type") == "code"]
    assert "if __name__ ==" not in "".join(nb04_code[0]["source"])
    assert "if __name__ ==" in "".join(nb04_code[-1]["source"])


def test_bootstrap_dataset_behavior_with_mock_kagglehub() -> None:
    """Testa is_directory_empty e bootstrap_dataset (cache, download, mount ro e cold start)."""
    for rel_path, expected_cold in PHASED_NOTEBOOKS:
        ns = _extract_bootstrap_namespace(REPO_ROOT / rel_path)
        assert ns["ALLOW_COLD_START"] is expected_cold
        assert ns["DATASET_PATH"] == "/data/export/tse-eleicoes-dados"
        assert ns["STAGING_PATH"] == "/tmp/tse-eleicoes-staging"

    ns = _extract_bootstrap_namespace(
        REPO_ROOT / "notebooks" / "02_ingest_electoral_tables" / "02_ingest_electoral_tables.ipynb"
    )
    is_directory_empty = ns["is_directory_empty"]
    bootstrap_dataset = ns["bootstrap_dataset"]
    assert callable(is_directory_empty)
    assert callable(bootstrap_dataset)

    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)

        # 1. Diretório vazio ou apenas com arquivos ocultos
        empty_dir = base / "empty_ds"
        empty_dir.mkdir()
        (empty_dir / ".gitkeep").write_text("", encoding="utf-8")
        assert is_directory_empty(empty_dir) is True
        assert is_directory_empty(base / "nonexistent") is True

        # 2. Diretório populado não dispara download
        populated_dir = base / "populated_ds"
        populated_dir.mkdir()
        (populated_dir / "dim_election.parquet").write_bytes(b"PAR1")
        assert is_directory_empty(populated_dir) is False

        download_calls: list[tuple[str, dict[str, object]]] = []

        def _fail_if_called(handle: str, **kwargs: object) -> str:
            download_calls.append((handle, kwargs))
            raise AssertionError(
                "dataset_download não deve ser chamado se DATASET_PATH não está vazio"
            )

        fake_kh = types.ModuleType("kagglehub")
        fake_kh.dataset_download = _fail_if_called  # type: ignore[attr-defined]
        old_kh = sys.modules.get("kagglehub")
        sys.modules["kagglehub"] = fake_kh
        try:
            res = bootstrap_dataset("miolodados/tse-eleicoes-dados", populated_dir, False)
            assert res == populated_dir
            assert download_calls == []

            # 3. Diretório vazio baixa e sincroniza mesmo se retornar mount read-only distinto
            ro_mount = base / "ro_mount"
            ro_mount.mkdir()
            (ro_mount / "fact_urna_logs").mkdir()
            (ro_mount / "fact_urna_logs" / "part-0.parquet").write_bytes(b"PAR1")

            def _mock_download(
                handle: str,
                output_dir: str | None = None,
                force_download: bool = False,
            ) -> str:
                download_calls.append(
                    (handle, {"output_dir": output_dir, "force_download": force_download})
                )
                return str(ro_mount)

            fake_kh.dataset_download = _mock_download  # type: ignore[attr-defined]
            target_dir = base / "target_ds"
            bootstrap_dataset("miolodados/tse-eleicoes-dados", target_dir, allow_cold_start=False)
            assert len(download_calls) == 1
            assert download_calls[0][1]["output_dir"] == str(target_dir)
            assert download_calls[0][1]["force_download"] is True
            assert (target_dir / "fact_urna_logs" / "part-0.parquet").read_bytes() == b"PAR1"

            # 4. Cold start vs erro fatal em 404
            def _mock_404(
                handle: str,
                output_dir: str | None = None,
                force_download: bool = False,
            ) -> str:
                raise RuntimeError("404 Client Error: Not Found")

            fake_kh.dataset_download = _mock_404  # type: ignore[attr-defined]
            cold_dir = base / "cold_ds"
            bootstrap_dataset("miolodados/tse-eleicoes-dados", cold_dir, allow_cold_start=True)
            assert is_directory_empty(cold_dir) is True

            try:
                bootstrap_dataset("miolodados/tse-eleicoes-dados", cold_dir, allow_cold_start=False)
            except RuntimeError as exc:
                assert "Falha ao popular DATASET_PATH" in str(exc)
            else:
                raise AssertionError("Esperado RuntimeError quando allow_cold_start=False")
        finally:
            if old_kh is None:
                sys.modules.pop("kagglehub", None)
            else:
                sys.modules["kagglehub"] = old_kh


def test_pyproject_includes_kagglehub() -> None:
    pyproject_text = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "kagglehub>=0.3.5" in pyproject_text


if __name__ == "__main__":
    os.environ.setdefault("PYTEST_CURRENT_TEST", "direct_runner")
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"[PASS] {name}")
    print("Todos os testes do contrato KaggleHub nos 9 notebooks passaram.")
