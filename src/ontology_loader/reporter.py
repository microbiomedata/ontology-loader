"""Reporting class to handle TSV dumping."""

import csv
import logging
import tempfile
from pathlib import Path
from typing import List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Report:

    """Class to hold report data."""

    def __init__(self, report_type: str, records: List[List[str]], headers: List[str]):
        """
        Initialize the Report object with the given report type, records, and headers.

        :param report_type: The type of report (e.g., "insertions", "updates")
        :param records: A list of records to include in the report
        :param headers: A list of headers to include in the report
        """
        self.report_type = report_type
        self.records = records
        self.headers = headers


class ReportWriter:

    """
    ReportWriter class to write reports to TSV files.

    This class provides a static method to write reports to TSV files in a given directory.
    """

    @staticmethod
    def write_reports(reports: List[Report], output_format: str = "tsv", output_directory: Optional[str] = None):
        """
        Write reports to a directory, creating one in a temporary location if not provided.

        :param reports: A list of Report objects to write to files
        :param output_format: The output format for the reports (default: "tsv")
        :param output_directory: The directory to write the reports to (default: None)
        """
        # Create a temporary directory if output_directory is None
        if output_directory is None:
            output_directory = Path(tempfile.mkdtemp(prefix="ontology_reports_"))
            logging.info(f"No output directory provided. Using temporary directory: {output_directory}")
        else:
            output_directory = Path(output_directory)
            output_directory.mkdir(parents=True, exist_ok=True)  # Ensure it exists

        # Iterate over reports and write each one to a file
        for report in reports:
            file_path = output_directory / f"ontology_{report.report_type}s.{output_format}"
            with file_path.open(mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t") if output_format == "tsv" else csv.writer(f)
                writer.writerow(["id"] + report.headers)
                writer.writerows(report.records)
            logging.info(f"Report generated: {file_path}")
