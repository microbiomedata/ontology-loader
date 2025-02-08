import gzip
import shutil
from pathlib import Path
from typing import List
import requests
from linkml_runtime import SchemaView
from nmdc_schema.nmdc import OntologyClass
import click
import pystow
from linkml_store import Client
import importlib.resources
import logging
from prefixmaps import load_context
from curies import Converter
from linkml.validator import Validator
from linkml.validator.plugins import PydanticValidationPlugin

validator = Validator(
    schema = str(importlib.resources.files('nmdc_schema').joinpath('nmdc_materialized_patterns.yaml'))
    # ,validation_plugins=[PydanticValidationPlugin()]
)

print (str(importlib.resources.files('nmdc_schema').joinpath('nmdc_materialized_patterns.yaml')))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_prefix_map() -> dict:
    """Initialize prefix map to contract URIs in xrefs to curies."""
    context = load_context("merged")
    extended_prefix_map = context.as_extended_prefix_map()
    converter = Converter.from_extended_prefix_map(extended_prefix_map)
    return converter.prefix_map


def find_schema() -> SchemaView:
    """Find the schema file path."""
    yaml_file_path = importlib.resources.files('nmdc_schema').joinpath('nmdc_materialized_patterns.yaml')
    sv = SchemaView(yaml_file_path)
    return sv


def connect_to_destination_store():
    """
    Initialize MongoDB using LinkML-store's client.

    :return: MongoDB client
    """

    client = Client()
    db = client.attach_database("mongodb", alias="nmdc", schema_view=find_schema())
    return db

def create_ontology_class(node: dict):
    """
    Create an OntologyClass object from a node in the ontology graph.

    :param node: A node in the ontology graph
    :return: An OntologyClass object
    """

    oclass = OntologyClass(
        id=node.get("id"),
        name=node.get("lbl"),
        type="nmdc:OntologyClass",
        alternative_identifiers=(node.get("xrefs") or []),
        alternative_names=(node.get("synonyms") or []),
        description=node.get("definition")
    )

    if validator.validate(oclass):
        return oclass
    else:
        logging.warning(f"Validation failed for {node.get('id')}")
        return None


def process_ontology_nodes(graph, converter) -> (List[dict], List[OntologyClass]):
    """
    Fetch metadata for all terms in an ontology and return a dictionary.

    :param graph: The ontology graph
    :param converter: The prefix_converter from prefixmaps using the "merged" context
    :return: A list of dictionaries of relevant metadata for each term including definition, synonyms, and xrefs
    """

    metadata = []
    class_instances = []

    for node in graph["nodes"]:
        node_id = node.get("id")
        meta = node.get("meta")
        if "ENVO" in node.get("id"):
            curie = converter.compress(str(node_id))
            if curie is None:
                logging.WARNING(f"Could not compress {node_id}")
                continue

            node_metadata = {"id": curie}
            if meta:
                xrefs = []
                syns = []
                definition = None
                if 'xrefs' in meta:
                    for xref in meta.get('xrefs', []):
                        val = xref.get("val", "")
                        if not (val.startswith("http") or " " in val):
                            xrefs.append(val)
                if 'synonyms' in meta:
                    for syn in meta.get("synonyms", []):
                        syns.append(syn.get("val", ""))
                if 'definition' in meta:
                    definition = meta.get("definition", "").get("val")
                node_metadata["alternative_identifiers"] = xrefs
                node_metadata["alternative_names"] = syns
                node_metadata["description"] = definition
                node_metadata["type"] = "nmdc:OntologyClass"
                node_metadata["name"] = node.get("lbl")
            valid_class = create_ontology_class(node_metadata)

            if not valid_class:
                logging.WARNING(f"Could not create OntologyClass for {node_id}")
            else:
                class_instances.append(valid_class)

            metadata.append(node_metadata)

    return metadata, class_instances


def insert_ontology_classes_into_db(db, term_dicts):
    """
    Insert each OntologyClass object into the 'ontology_class_set' collection using linkml-store.

    :param db: The MongoDB database object
    :param term_dicts: A list of OntologyClass objects to insert
    """

    # Ensure the collection is created and ready
    collection = db.create_collection('ontology_class_set', recreate_if_exists=True)

    # Insert OntologyClass objects into the MongoDB collection
    if term_dicts:
        collection.insert(term_dicts)  # insert method expects a list of LinkML objects
        print(f"Inserted {len(term_dicts)} OntologyClass objects into the 'ontology_class_set' collection.")
    else:
        print("No OntologyClass objects to insert.")


@click.command()
@click.option('--db-url', default='mongodb://localhost:27017', help='MongoDB connection URL')
@click.option('--db-name', default='nmdc', help='Database name')
@click.option('--source-ontology', default='envo', help='Lowercase ontology prefix, e.g., envo, go, uberon, etc.')
def main(db_url, db_name, source_ontology):
    """Main function to process ontology and store metadata, ensuring the ontology database is available."""

    # Get the ontology-specific pystow directory
    source_ontology_module = pystow.module(source_ontology).base  # This is ~/.pystow/envo (or another module)

    # If the directory exists, remove it and all its contents
    if source_ontology_module.exists():
        print(f"Removing existing pystow directory for {source_ontology}: {source_ontology_module}")
        shutil.rmtree(source_ontology_module)

    # Define ontology URL
    ontology_db_url_prefix = 'https://s3.amazonaws.com/bbop-sqlite/'
    ontology_db_url_suffix = '.db.gz'
    ontology_url = ontology_db_url_prefix + source_ontology + ontology_db_url_suffix

    # Define paths (download to the module-specific directory)
    compressed_path = pystow.ensure(source_ontology, f"{source_ontology}.db.gz", url=ontology_url)
    decompressed_path = compressed_path.with_suffix('')  # Remove .gz to get .db file

    # Extract the file if not already extracted
    if not decompressed_path.exists():
        print(f"Extracting {compressed_path} to {decompressed_path}...")
        with gzip.open(compressed_path, 'rb') as f_in:
            with open(decompressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    print(f"Ontology database is ready at: {decompressed_path}")

    # Load the ontology graph
    graphdoc = json.load(open(path))
    graph = graphdoc["graphs"][0]
    print(f"Processing {len(graph['nodes'])} nodes and {len(graph['edges'])} edges...")

    # Initialize the prefix map and converter
    cmaps = initialize_prefix_map()
    converter = Converter.from_prefix_map(cmaps, strict=False)

    # Process ontology nodes
    term_dicts, _ = process_ontology_nodes(graph, converter)


    # Connect to the database
    db_client = connect_to_destination_store()

    # Insert OntologyClass objects into MongoDB using linkml-store
    insert_ontology_classes_into_db(db_client, term_dicts)

    # Print the completion message
    print("Processing complete. Data inserted into the database.")


if __name__ == "__main__":
    main()
