"""Ontology Processor class to process ontology terms and relations."""

import gzip
import logging
import shutil

import pystow
from linkml_runtime.dumpers import json_dumper
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from oaklib import get_adapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class OntologyProcessor:

    """Ontology Processor class to process ontology terms and relations."""

    def __init__(self, ontology: str):
        """
        Initialize the OntologyProcessor with a given SQLite ontology.

        :param ontology: The ontology prefix (e.g., "envo", "go", "uberon", etc.)

        """
        self.ontology = ontology
        self.ontology_db_path = self.download_and_prepare_ontology()
        self.adapter = get_adapter(f"sqlite:{self.ontology_db_path}")
        self.adapter.precompute_lookups()  # Optimize lookups

    def download_and_prepare_ontology(self):
        """Download and prepare the ontology database for processing."""
        logger.info(f"Preparing ontology: {self.ontology}")

        # Get the ontology-specific pystow directory
        source_ontology_module = pystow.module(self.ontology).base  # Example: ~/.pystow/envo

        # If the directory exists, remove it and all its contents
        if source_ontology_module.exists():
            logger.info(f"Removing existing pystow directory for {self.ontology}: {source_ontology_module}")
            shutil.rmtree(source_ontology_module)

        # Define ontology URL
        ontology_db_url_prefix = "https://s3.amazonaws.com/bbop-sqlite/"
        ontology_db_url_suffix = ".db.gz"
        ontology_url = ontology_db_url_prefix + self.ontology + ontology_db_url_suffix

        # Define paths (download to the module-specific directory)
        compressed_path = pystow.ensure(self.ontology, f"{self.ontology}.db.gz", url=ontology_url)
        decompressed_path = compressed_path.with_suffix("")  # Remove .gz to get .db file

        # Extract the file if not already extracted
        if not decompressed_path.exists():
            logger.info(f"Extracting {compressed_path} to {decompressed_path}...")
            with gzip.open(compressed_path, "rb") as f_in:
                with open(decompressed_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

        logger.info(f"Ontology database is ready at: {decompressed_path}")
        return decompressed_path

    def get_terms_and_metadata(self):
        """Retrieve all terms that start with the ontology prefix and return a list of OntologyClass objects."""
        ontology_classes = []

        for entity in self.adapter.entities(filter_obsoletes=True):
            if entity.startswith(self.ontology.upper() + ":"):
                ontology_class = OntologyClass(
                    id=entity,
                    type="nmdc:OntologyClass",
                    alternative_names=self.adapter.entity_aliases(entity) or [],
                    definition=self.adapter.definition(entity) or "",
                    relations=[],
                )

                ontology_classes.append(ontology_class)

        for obolete_entity in self.adapter.obsoletes():
            if obolete_entity.startswith(self.ontology.upper() + ":"):
                ontology_class = OntologyClass(
                    id=obolete_entity,
                    type="nmdc:OntologyClass",
                    alternative_names=self.adapter.entity_aliases(obolete_entity) or [],
                    definition=self.adapter.definition(obolete_entity) or "",
                    relations=[],
                    is_obsolete=True,
                )

                ontology_classes.append(ontology_class)

        return ontology_classes

    def _create_relation(self, subject, predicate, obj, ontology_terms_dict):
        """
        Create an ontology relation and update related ontology terms.

        :param subject: Subject of the relation
        :param predicate: Predicate of the relation
        :param obj: Object of the relation
        :param ontology_terms_dict: Dictionary of ontology terms for fast lookup
        :return: Dictionary representation of the relation
        """
        ontology_relation = OntologyRelation(
            subject=subject,
            predicate=predicate,
            object=obj,
            type="nmdc:OntologyRelation",
        )

        # Update the term's relations list if it exists in our dictionary
        if subject in ontology_terms_dict:
            ontology_terms_dict[subject].relations.append(ontology_relation)

        # Convert and return the relation dictionary
        return json_dumper.to_dict(ontology_relation)

    def get_relations_closure(self, predicates=None, ontology_terms: list = None) -> tuple:
        """
        Retrieve all ontology relations closure for terms with improved performance.

        :param predicates: List of predicates to consider (default: ["rdfs:subClassOf", "BFO:0000050"])
        :param ontology_terms: List of OntologyClass objects to consider (default: None)
        :return: Tuple of (ontology_relations, updated_ontology_terms)
        """
        predicates = ["rdfs:subClassOf", "BFO:0000050"] if predicates is None else predicates
        ontology_prefix = self.ontology.upper() + ":"
        ontology_relations = []

        # Create dictionary for fast lookup of ontology terms
        ontology_terms_dict = {term.id: term for term in (ontology_terms or [])}

        # Get all relevant entities in one pass
        logger.info("Collecting relevant entities...")
        relevant_entities = set(
            entity for entity in self.adapter.entities()
            if entity.startswith(ontology_prefix)
        )
        logger.info(f"Found {len(relevant_entities)} relevant entities")

        # Process all direct relationships in one batch
        logger.info("Processing direct relationships...")
        relationship_count = 0
        predicate_set = set(predicates)  # Convert to set for faster lookups

        # Get all relationships at once and filter as we process them
        for subject, predicate, obj in self.adapter.relationships():
            if subject in relevant_entities and predicate in predicate_set:
                relation_dict = self._create_relation(subject, predicate, obj, ontology_terms_dict)
                ontology_relations.append(relation_dict)
                relationship_count += 1

        logger.info(f"Processed {relationship_count} direct relationships")

        # Process all ancestors for all entities in one batch
        logger.info("Processing ancestry relationships...")
        ancestry_count = 0

        for entity in relevant_entities:
            # Get ancestors for this entity and filter to only include those from our ontology
            ancestors = set(
                ancestor for ancestor in self.adapter.ancestors(entity, reflexive=True, predicates=predicates)
                if ancestor.startswith(ontology_prefix)
            )

            # Create relations for each ancestor
            for ancestor in ancestors:
                relation_dict = self._create_relation(
                    entity, "entailed_isa_partof_closure", ancestor, ontology_terms_dict
                )
                ontology_relations.append(relation_dict)
                ancestry_count += 1

        logger.info(f"Processed {ancestry_count} ancestry relationships")
        logger.info(f"Total relations: {len(ontology_relations)}")

        # Return the relations and updated ontology terms
        return ontology_relations, list(ontology_terms_dict.values())


