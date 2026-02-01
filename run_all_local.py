from __future__ import annotations

import subprocess
import sys

"""
Convenience runner for local testing.

In production, orchestration is typically done by Azure Data Factory + Databricks jobs,
but this helps you debug end-to-end logic quickly.
"""

def run(cmd: list[str]) -> None:
    print("Running:", " ".join(cmd))
    subprocess.check_call(cmd)

def main() -> None:
    run([sys.executable, "pipelines/run_extract.py"])
    # Transform step is a spark job in real usage, so we only show command template.
    print("\nTransform is a PySpark job. Example:")
    print("  spark-submit pipelines/run_transform.py --run-date $RUN_DATE --abfss-prefix abfss://<container>@<account>.dfs.core.windows.net")
    run([sys.executable, "pipelines/run_load.py"])

if __name__ == "__main__":
    main()
