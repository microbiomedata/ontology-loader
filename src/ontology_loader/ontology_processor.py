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


def _create_relation(subject, predicate, obj, ontology_terms_dict):
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

        # Cache root terms for efficient lookups
        self.root_terms = set(self.adapter.roots())

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

    def _create_ontology_class(self, entity_id, is_obsolete=False):
        """
        Create an OntologyClass instance with common attributes.

        :param entity_id: The entity ID for the ontology class
        :param is_obsolete: Whether the entity is obsolete
        :return: An OntologyClass instance
        """
        ontology_class = OntologyClass(
            id=entity_id,
            type="nmdc:OntologyClass",
            alternative_names=self.adapter.entity_aliases(entity_id) or [],
            definition=self.adapter.definition(entity_id) or "",
            relations=[],
            is_root=entity_id in self.root_terms,
            is_obsolete=is_obsolete,
            name=self.adapter.label(entity_id) or "",
        )

        # Ensure boolean values are properly set
        if ontology_class.is_root is None:
            ontology_class.is_root = False
        if ontology_class.is_obsolete is None:
            ontology_class.is_obsolete = is_obsolete

        return ontology_class

    def get_terms_and_metadata(self):
        """Retrieve all terms that start with the ontology prefix and return a list of OntologyClass objects."""
        ontology_classes = []
        ontology_prefix = self.ontology.upper() + ":"

        # Process non-obsolete entities
        for entity in self.adapter.entities(filter_obsoletes=True):
            if entity.startswith(ontology_prefix):
                ontology_class = self._create_ontology_class(entity, is_obsolete=False)
                ontology_classes.append(ontology_class)

        # Process obsolete entities
        for obsolete_entity in self.adapter.obsoletes():
            if obsolete_entity.startswith(ontology_prefix):
                ontology_class = self._create_ontology_class(obsolete_entity, is_obsolete=True)
                ontology_classes.append(ontology_class)

        return ontology_classes

    def get_relations_closure(
        self,
        predicates=None,
        ontology_terms: list = None,
        emit_combined_closure: bool = True,
        emit_isa_closure: bool = False,
        emit_partof_closure: bool = False,
    ) -> tuple:
        """
        Retrieve all ontology relations closure for terms with improved performance.

        :param predicates: Predicates to filter the direct-edge phase by (default:
            ["rdfs:subClassOf", "BFO:0000050"]). Does not affect which entailed
            closures are emitted; that is controlled by the three emit_* flags.
        :param ontology_terms: List of OntologyClass objects to consider (default: None)
        :param emit_combined_closure: If True (default), emit
            `entailed_isa_partof_closure` relations covering the combined
            transitive closure over `rdfs:subClassOf` ∪ `BFO:0000050`.
            Backward-compatible default.
        :param emit_isa_closure: If True, additionally emit `entailed_isa_closure`
            relations covering the transitive closure over `rdfs:subClassOf` only.
        :param emit_partof_closure: If True, additionally emit `entailed_partof_closure`
            relations covering the transitive closure over `BFO:0000050` only.
        :return: Tuple of (ontology_relations, updated_ontology_terms)
        """
        predicates = ["rdfs:subClassOf", "BFO:0000050"] if predicates is None else predicates
        ontology_prefix = self.ontology.upper() + ":"
        ontology_relations = []

        # Create dictionary for fast lookup of ontology terms
        ontology_terms_dict = {term.id: term for term in (ontology_terms or [])}

        # Get all relevant entities in one pass
        logger.info("Collecting relevant entities...")
        relevant_entities = set(entity for entity in self.adapter.entities() if entity.startswith(ontology_prefix))
        logger.info(f"Found {len(relevant_entities)} relevant entities")

        # Process all direct relationships in one batch
        logger.info("Processing direct relationships...")
        relationship_count = 0
        predicate_set = set(predicates)  # Convert to set for faster lookups

        # Get all relationships at once and filter as we process them
        for subject, predicate, obj in self.adapter.relationships():
            if subject in relevant_entities and predicate in predicate_set:
                relation_dict = _create_relation(subject, predicate, obj, ontology_terms_dict)
                ontology_relations.append(relation_dict)
                relationship_count += 1

        logger.info(f"Processed {relationship_count} direct relationships")

        # Build the list of (output_predicate_name, ancestry_predicates) closures
        # the caller asked for. Each requires its own ancestors() traversal per entity.
        closure_specs = []
        if emit_combined_closure:
            closure_specs.append(("entailed_isa_partof_closure", ["rdfs:subClassOf", "BFO:0000050"]))
        if emit_isa_closure:
            closure_specs.append(("entailed_isa_closure", ["rdfs:subClassOf"]))
        if emit_partof_closure:
            closure_specs.append(("entailed_partof_closure", ["BFO:0000050"]))

        if closure_specs:
            logger.info(
                f"Processing ancestry relationships across {len(closure_specs)} closure type(s): "
                f"{', '.join(name for name, _ in closure_specs)}"
            )
            ancestry_count = 0
            for entity in relevant_entities:
                for closure_name, closure_predicates in closure_specs:
                    ancestors = set(
                        ancestor
                        for ancestor in self.adapter.ancestors(
                            entity, reflexive=True, predicates=closure_predicates
                        )
                        if ancestor.startswith(ontology_prefix)
                    )
                    for ancestor in ancestors:
                        relation_dict = _create_relation(
                            entity, closure_name, ancestor, ontology_terms_dict
                        )
                        ontology_relations.append(relation_dict)
                        ancestry_count += 1
            logger.info(f"Processed {ancestry_count} ancestry relationships")
        else:
            logger.info("No ancestry closure types enabled; skipping ancestry computation.")

        logger.info(f"Total relations: {len(ontology_relations)}")

        # Return the relations and updated ontology terms
        return ontology_relations, list(ontology_terms_dict.values())
