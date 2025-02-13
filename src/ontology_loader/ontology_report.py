"""Reporting class to handle TSV dumping."""

import csv
import logging
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class Report:

    """Dataclass to hold report data."""

    report_type: str  # "update" or "insert"
    records: List[List[str]]
    headers: List[str]


class ReportWriter:

    """ReportWriter class to write reports to TSV files."""

    @staticmethod
    def write_reports(reports: List[Report], output_format: str = "tsv", output_directory: Optional[str] = None):
        """Write reports to TSV files."""
        if output_directory is None:
            output_directory = Path(tempfile.gettempdir())
        else:
            output_directory = Path(output_directory)

        for report in reports:
            file_path = output_directory / f"ontology_{report.report_type}s.{output_format}"
            with file_path.open(mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t") if output_format == "tsv" else csv.writer(f)
                writer.writerow(["id"] + report.headers)
                writer.writerows(report.records)
            logging.info(f"Report generated: {file_path}")