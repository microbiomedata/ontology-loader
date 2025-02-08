import pystow
import shutil
import gzip
from pathlib import Path
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
        Retrieve all ontology terms that start with 'ENVO:' along with metadata.
        """
        for entity in self.adapter.entities():
            if entity.startswith(self.ontology.upper() + ":"):
                print(entity)
                print(self.adapter.entity_aliases(entity))
                print(self.adapter.definition(entity))

    def get_relations_closure(self, predicates=None):
        """
        Retrieve all ontology relations closure for terms starting with 'ENVO:'.
        Defaults to 'rdfs:subClassOf' and 'BFO:0000050'.
        """
        predicates = ["rdfs:subClassOf", "BFO:0000050"] if predicates is None else predicates

        for entity in self.adapter.entities():
            if entity.startswith("ENVO:"):
                # Convert generator to list
                ancestors_list = list(self.adapter.ancestors(entity, reflexive=True, predicates=predicates))

                # Filter to keep only ENVO terms
                filtered_ancestors = list(set(a for a in ancestors_list if a.startswith("ENVO:")))

                if filtered_ancestors:  # Print only if the list is non-empty
                    print(f"{entity} -> {filtered_ancestors}")


