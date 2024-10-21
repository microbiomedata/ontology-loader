
from typing import List
from nmdc_schema.nmdc import OntologyClass
import click
import pystow
import json
from linkml_store import Client
from linkml_store.api.config import ClientConfig
import importlib.resources
import logging
from prefixmaps import load_context
from curies import Converter
from linkml_store.api.database import Database

logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_prefix_map() -> dict:
    """Initialize prefix map to contract URIs in xrefs to curies."""
    context = load_context("merged")
    extended_prefix_map = context.as_extended_prefix_map()
    converter = Converter.from_extended_prefix_map(extended_prefix_map)
    return converter.prefix_map


def find_schema():
    """Find the schema file path."""
    yaml_file_path = importlib.resources.files('nmdc_schema').joinpath('nmdc_materialized_patterns.yaml')
    return str(yaml_file_path)


def connect_to_destination_store():
    """
    Initialize MongoDB using LinkML-store's client.

    :return: MongoDB client
    """

    client = Client()
    db = client.attach_database("mongodb://nmdc", "nmdc")
    return db

def create_ontology_class(node: dict):
    """
    Create an OntologyClass object from a node in the ontology graph.

    :param node: A node in the ontology graph
    :return: An OntologyClass object
    """

    return OntologyClass(
        id=node.get("id"),
        name=node.get("lbl"),
        type="nmdc:OntologyClass",
        alternative_identifiers=(node.get("xrefs") or []),
        alternative_names=(node.get("synonyms") or []),
        description=node.get("definition")
    )


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
        curie = None
        if "ENVO" in node.get("id"):
            curie = converter.compress(str(node_id))
            if curie is None:
                logging.WARNING(f"Could not compress {node_id}")
                continue

            node_metadata = {"id": curie}
            print("curie:", curie)
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
@click.option('--db-name', default='test', help='Database name')
@click.option('--source-ontology-url', default='https://purl.obolibrary.org/obo/envo.json', help='Ontology URL')
def main(db_url, db_name, source_ontology_url):
    """Main function to process ontology and store metadata."""
    # Download the ontology file
    path = pystow.ensure("tmp", "envo.json", url=source_ontology_url)

    # Load the ontology graph
    graphdoc = json.load(open(path))
    graph = graphdoc["graphs"][0]
    print(f"Processing {len(graph['nodes'])} nodes and {len(graph['edges'])} edges...")

    # Initialize the prefix map and converter
    cmaps = initialize_prefix_map()
    converter = Converter.from_prefix_map(cmaps, strict=False)

    # Process ontology nodes
    term_dicts, term_classes = process_ontology_nodes(graph, converter)


    # Connect to the database
    db_client = connect_to_destination_store()

    # Insert OntologyClass objects into MongoDB using linkml-store
    insert_ontology_classes_into_db(db_client, term_dicts)

    # Print the completion message
    print("Processing complete. Data inserted into the database.")


if __name__ == "__main__":
    main()
