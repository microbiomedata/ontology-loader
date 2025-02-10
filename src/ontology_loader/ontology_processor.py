import pystow
import shutil
import gzip

from linkml_runtime.dumpers import json_dumper
from nmdc_schema.nmdc import OntologyClass, OntologyRelation
from oaklib import get_adapter


class OntologyProcessor:
    def __init__(self, ontology: str):
        """
        Initialize the OntologyProcessor with a given SQLite ontology.
        """
        self.ontology = ontology
        self.ontology_db_path = self.download_and_prepare_ontology()
        self.adapter = get_adapter(f"sqlite:{self.ontology_db_path}")
        self.adapter.precompute_lookups()  # Optimize lookups

    def download_and_prepare_ontology(self):
        """
        Ensures the ontology database is available by downloading and extracting it if necessary.
        """
        print(f"Preparing ontology: {self.ontology}")

        # Get the ontology-specific pystow directory
        source_ontology_module = pystow.module(self.ontology).base  # Example: ~/.pystow/envo

        # If the directory exists, remove it and all its contents
        if source_ontology_module.exists():
            print(f"Removing existing pystow directory for {self.ontology}: {source_ontology_module}")
            shutil.rmtree(source_ontology_module)

        # Define ontology URL
        ontology_db_url_prefix = 'https://s3.amazonaws.com/bbop-sqlite/'
        ontology_db_url_suffix = '.db.gz'
        ontology_url = ontology_db_url_prefix + self.ontology + ontology_db_url_suffix

        # Define paths (download to the module-specific directory)
        compressed_path = pystow.ensure(self.ontology, f"{self.ontology}.db.gz", url=ontology_url)
        decompressed_path = compressed_path.with_suffix('')  # Remove .gz to get .db file

        # Extract the file if not already extracted
        if not decompressed_path.exists():
            print(f"Extracting {compressed_path} to {decompressed_path}...")
            with gzip.open(compressed_path, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        print(f"Ontology database is ready at: {decompressed_path}")
        return decompressed_path

    def get_terms_and_metadata(self):
        """
        Retrieve all ontology terms that start with the ontology prefix and return a list of OntologyClass objects.
        """
        ontology_classes = []

        for entity in self.adapter.entities():
            if entity.startswith(self.ontology.upper() + ":"):
                ontology_class = OntologyClass(
                    id=entity,
                    type="nmdc:OntologyClass",
                    alternative_names=self.adapter.entity_aliases(entity) or [],
                    definition=self.adapter.definition(entity) or "",
                )

                ontology_classes.append(ontology_class)

        return ontology_classes

    def get_relations_closure(self, predicates=None):
        """
        Retrieve all ontology relations closure for terms starting with the ontology prefix
        and return a list of dictionaries conforming to the OntologyRelation JSON structure.
        """
        predicates = ["rdfs:subClassOf", "BFO:0000050"] if predicates is None else predicates
        ontology_relations = []

        for entity in self.adapter.entities():
            if entity.startswith(self.ontology.upper() + ":"):
                # Convert generator to list
                ancestors_list = list(self.adapter.ancestors(entity, reflexive=True, predicates=predicates))

                # Filter to keep only ENVO terms
                filtered_ancestors = list(set(a for a in ancestors_list if a.startswith(self.ontology.upper() + ":")))

                for ancestor in filtered_ancestors:
                    ontology_relation = OntologyRelation(
                        subject=entity,
                        predicate="is_a",  # TODO: fix this to the real predicate that it came with
                        object=ancestor,
                        type="nmdc:OntologyRelation"
                    )

                    # Convert OntologyRelation instance to a dictionary
                    ontology_relations.append(json_dumper.to_dict(ontology_relation))

        return ontology_relations


