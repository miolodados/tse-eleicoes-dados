# Project Rules & Development Guidelines: tse-eleicoes-dados

## 1. Parquet Dataset & Ingestion Standards
- **Rolling File Sharding:** Keep individual Parquet files under 128 MB (default: `134217728` bytes, configurable via `TSE_MAX_PARQUET_PART_BYTES`).
- **Urna Boundary Invariant:** A `.jez` archive (representing an individual urna) must never be split across multiple Parquet part files. Always test boundary capacity before appending the next complete urna.
- **Partition Naming:** Multi-part partitions must be named sequentially (`part-0.parquet`, `part-1.parquet`, etc.) so downstream readers (`DuckDB`, `Polars`, `Spark`) discover them natively via wildcard `*.parquet`.
- **DuckDB 2.0 Compatibility:** Do not call `.fetchone()` on DuckDB `Relation` objects. Use `duckdb.connect(":memory:").execute(query).fetchone()` or convert to Arrow/DataFrame.

## 2. Google Drive FUSE Filesystem Safety
- **No In-Place Overwrites on Drive:** Never perform `os.replace()` or overwrite existing files directly on `/content/drive/MyDrive/...`. Drive FUSE can preserve previous inodes if file handles linger, causing duplicate data.
- **Staging Pattern:** Always stage Parquet writes locally in `/tmp/tse-eleicoes-staging/` first. Clean up destination folders on Drive completely before moving completed files from staging.
- **Atomic Manifest Updates:** Update `_checkpoints/manifest.json` using a temporary file (`.json.tmp`) and atomic rename, ensuring physical files on Drive match manifest partition records 1:1.

## 3. Google Colab Session Management
- **Preserve Drive Mounts:** Never run `colab stop -s <session>` to interrupt or restart a task; doing so releases the VM and loses the interactive Google Drive mount. Use `colab restart-kernel -s <session>` to reset state while keeping `/content/drive` mounted.
- **CLI Permissions:** In agent environments, invoke `colab` CLI commands with sandbox bypass (`BypassSandbox: true`) because the CLI requires access to `~/.config/colab-cli` credentials and remote Google endpoints.
