"""Make the collector modules available to standalone tools."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "collector"))
