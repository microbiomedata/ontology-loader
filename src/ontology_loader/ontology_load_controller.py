"""Controller that orchestrates OntologyProcessor + MongoDBLoader for one or more ontologies."""

import logging
import tempfile
import warnings

from ontology_loader.mongodb_loader import MongoDBLoader
from ontology_loader.ontology_processor import OntologyProcessor
from ontology_loader.reporter import ReportWriter
from ontology_loader.utils import load_yaml_from_package

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


VALID_MODES = ("meticulous", "fast-initial")

# Sentinel for "the caller did not pass this kwarg" — distinct from passing None or any literal.
_UNSET = object()


class OntologyLoaderController:

    """
    Orchestrates extraction + loading of one or more ontologies into MongoDB.

    Migrating from 0.2.x
    ====================

    The constructor now accepts a richer set of kwargs and keeps the 0.2.x ones working as deprecated
    aliases. Each deprecated kwarg emits a ``DeprecationWarning`` and either no-ops or maps onto a new
    kwarg. See ``CHANGELOG.md`` and the README's "Migrating from 0.2.x" section for the full table.

    +-------------------------+------------------------------+-------------------------------------------------+
    | old kwarg               | new kwarg                    | behavior                                        |
    +=========================+==============================+=================================================+
    | source_ontology=<str>   | source_ontology=<str|list>   | unchanged; now also accepts a list of strings   |
    +-------------------------+------------------------------+-------------------------------------------------+
    | output_directory=<str>  | report_directory=<str>       | renamed; old kwarg is an alias with             |
    |                         |                              | DeprecationWarning. Passing both raises.        |
    +-------------------------+------------------------------+-------------------------------------------------+
    | generate_reports=True   | (gone — implicit)            | no-op with DeprecationWarning (True was always  |
    |                         |                              | the default).                                   |
    +-------------------------+------------------------------+-------------------------------------------------+
    | generate_reports=False  | mode='fast-initial'          | mapped to ``mode='fast-initial'`` with          |
    |                         |                              | DeprecationWarning. If ``mode`` was also passed |
    |                         |                              | explicitly, raises.                             |
    +-------------------------+------------------------------+-------------------------------------------------+
    | (none)                  | mode='meticulous'            | new kwarg. Default ``'meticulous'`` preserves   |
    |                         |                              | 0.2.x behavior (per-item upsert via             |
    |                         |                              | linkml-store + TSV reports).                    |
    +-------------------------+------------------------------+-------------------------------------------------+
    | (none)                  | closure='combined'           | new kwarg. Default ``'combined'`` preserves     |
    |                         |                              | 0.2.x behavior. Accepts a string or list;       |
    |                         |                              | values ``'combined'``, ``'isa'``, ``'partof'``, |
    |                         |                              | ``'all'``, ``'none'``.                          |
    +-------------------------+------------------------------+-------------------------------------------------+
    """

    def __init__(
        self,
        source_ontology="envo",
        output_directory=_UNSET,
        generate_reports=_UNSET,
        mongo_client=None,
        db_name: str = None,
        report_directory: str = None,
        mode: str = "meticulous",
        closure="combined",
    ):
        """
        Set the parameters for the OntologyLoader.

        :param source_ontology: Ontology prefix string (``'envo'``) or a list of prefixes
            (``['envo', 'po', 'uberon']``) processed sequentially in given order.
        :param output_directory: DEPRECATED. Use ``report_directory`` instead.
        :param generate_reports: DEPRECATED. ``True`` is a no-op; ``False`` maps to ``mode='fast-initial'``.
        :param mongo_client: Optional existing MongoDB client to use instead of creating a new connection.
        :param db_name: Database name to use with existing client (required when ``mongo_client`` is provided).
        :param report_directory: Where to write TSV reports (only used when ``mode='meticulous'``).
            Defaults to a fresh tempdir.
        :param mode: ``'meticulous'`` (default) — pure linkml-store, per-item upsert, TSV reports;
            matches 0.2.x behavior. ``'fast-initial'`` — raw pymongo ``insert_many``, no upsert, no reports;
            for first-time installs of large ontologies.
        :param closure: Closure spec (string or list of strings) from
            ``{'combined', 'isa', 'partof', 'all', 'none'}``. See ``OntologyProcessor.get_relations_closure``.
        """
        if mode not in VALID_MODES:
            raise ValueError(f"Unknown mode {mode!r}; expected one of {VALID_MODES}.")

        # Handle the deprecation pathway for output_directory → report_directory.
        if output_directory is not _UNSET:
            if report_directory is not None:
                raise ValueError("Pass either report_directory= (new) or output_directory= (deprecated), not both.")
            warnings.warn(
                "OntologyLoaderController(output_directory=...) is deprecated; use report_directory= instead. "
                "See 'Migrating from 0.2.x' in the README and CHANGELOG.md. Aliasing for this run.",
                DeprecationWarning,
                stacklevel=2,
            )
            report_directory = output_directory

        # Handle the deprecation pathway for generate_reports → mode='fast-initial'.
        # Passing mode='meticulous' explicitly is benign because that's where generate_reports=False
        # would have landed anyway under 0.2.x; only flag a conflict if mode != 'meticulous'.
        if generate_reports is not _UNSET:
            if generate_reports is False:
                if mode != "meticulous":
                    raise ValueError(
                        f"generate_reports=False maps to mode='fast-initial' but you also passed "
                        f"mode={mode!r}. Drop generate_reports and pass only mode."
                    )
                warnings.warn(
                    "OntologyLoaderController(generate_reports=False) is deprecated; "
                    "use mode='fast-initial' instead. Treating this run as mode='fast-initial'. "
                    "See 'Migrating from 0.2.x' in the README and CHANGELOG.md.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                mode = "fast-initial"
            elif generate_reports is True:
                warnings.warn(
                    "OntologyLoaderController(generate_reports=True) is deprecated and a no-op "
                    "(True was always the default). Drop it from your call. "
                    "See 'Migrating from 0.2.x' in the README and CHANGELOG.md.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            else:
                raise ValueError(f"generate_reports must be True or False (or omitted); got {generate_reports!r}.")

        # Normalize source_ontology to a list[str].
        if isinstance(source_ontology, str):
            self.source_ontologies = [source_ontology]
        else:
            self.source_ontologies = list(source_ontology)
        if not self.source_ontologies:
            raise ValueError("source_ontology must include at least one ontology name.")

        self.report_directory = report_directory if report_directory is not None else tempfile.gettempdir()
        self.mongo_client = mongo_client
        self.db_name = db_name
        self.mode = mode
        self.closure = closure

        # Validate that db_name is provided when mongo_client is provided
        if self.mongo_client and not self.db_name:
            raise ValueError("Database name (db_name) is required when providing a MongoDB client")

    # ---- 0.2.x-shape compatibility shims -------------------------------------------------------
    @property
    def source_ontology(self):
        """0.2.x compatibility: scalar accessor for the first source ontology."""
        return self.source_ontologies[0]

    @property
    def output_directory(self):
        """0.2.x compatibility alias for ``report_directory``."""
        return self.report_directory

    # --------------------------------------------------------------------------------------------

    def run_ontology_loader(self):
        """Process each requested ontology in turn; route writes to the configured mode."""
        nmdc_sv = load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")

        # Pystow cache discipline: meticulous forces a fresh download; fast-initial reuses if present.
        force_refresh = self.mode == "meticulous"

        # Open one MongoDBLoader and reuse it across all requested ontologies.
        db_manager = MongoDBLoader(schema_view=nmdc_sv, mongo_client=self.mongo_client, db_name=self.db_name)
        if not self.mongo_client:
            logger.info(f"Db port {db_manager.db_config.db_port}")
            logger.info(f"MongoDB host {db_manager.db_config.db_host}")

        for source_ontology in self.source_ontologies:
            logger.info(f"=== Loading ontology: {source_ontology} (mode={self.mode}, closure={self.closure}) ===")
            processor = OntologyProcessor(source_ontology, force_refresh=force_refresh)

            ontology_classes = processor.get_terms_and_metadata()
            logger.info(f"Extracted {len(ontology_classes)} ontology classes from {source_ontology}.")

            ontology_relations, ontology_classes_relations = processor.get_relations_closure(
                closure=self.closure,
                ontology_terms=ontology_classes,
            )
            logger.info(f"Extracted {len(ontology_relations)} ontology relations from {source_ontology}.")

            if self.mode == "meticulous":
                updates_report, insertions_report, insert_relations_report = db_manager.upsert_ontology_data(
                    ontology_classes_relations,
                    ontology_relations,
                )
                ReportWriter.write_reports(
                    reports=[updates_report, insertions_report, insert_relations_report],
                    output_format="tsv",
                    output_directory=self.report_directory,
                )
            else:  # fast-initial
                db_manager.insert_ontology_data_fast_initial(
                    ontology_classes_relations,
                    ontology_relations,
                )

            logger.info(f"=== Finished {source_ontology} ===")

        logger.info("Processing complete. Data inserted into MongoDB.")


if __name__ == "__main__":
    """Run the OntologyLoader."""
    OntologyLoaderController().run_ontology_loader()
