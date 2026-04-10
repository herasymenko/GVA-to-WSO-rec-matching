from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from loader import discover_files


@dataclass
class IngestionResult:
    rec_files: list[Path]
    mapping_file: Path
    key_values_file: Path
    elapsed_ms: int


def discover_inputs(project_root: Path) -> IngestionResult:
    started = perf_counter()
    files = discover_files(project_root)

    result = IngestionResult(
        rec_files=files.rec_files,
        mapping_file=files.mapping_file,
        key_values_file=files.key_values_file,
        elapsed_ms=int((perf_counter() - started) * 1000),
    )
    return result


def run_ingestion_stage(project_root: Path) -> int:
    result = discover_inputs(project_root)

    print("[ingestion] status=ok")
    print(f"[ingestion] rec_files={','.join(p.name for p in result.rec_files)}")
    print(f"[ingestion] mapping_file={result.mapping_file.name}")
    print(f"[ingestion] key_values_file={result.key_values_file.name}")
    print(f"[ingestion] total_elapsed_ms={result.elapsed_ms}")
    return 0
