import click
import pystow
import json
from linkml_store import Client
from linkml_store.api.config import ClientConfig
import importlib.resources
from prefixmaps import load_context
from curies import Converter


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


def connect_to_destination_store(db_url, db_name):
    """Initialize MongoDB using LinkML-store's client."""
    client_config = ClientConfig(handle=db_url, schema_path=find_schema())
    client = Client(handle=client_config.handle, metadata=client_config)
    return client


def process_ontology_nodes(graph, converter):
    """Process ontology nodes and extract metadata."""
    for node in graph["nodes"]:
        node_id = node.get("id")
        meta = node.get("meta")
        if "ENVO" in node.get("id"):
            curie = converter.compress(str(node_id))
            print("curie:", curie)
            if meta:
                if 'xrefs' in meta:
                    for xref in meta.get('xrefs', []):
                        val = xref.get("val", "")
                        if not (val.startswith("http") or " " in val):
                            print("xref:", val)
                if 'synonyms' in meta:
                    for syn in meta.get("synonyms", []):
                        print("synonym: ", syn.get("val", ""))
                if 'definition' in meta:
                    print("definition: ", meta.get("definition", "").get("val"))


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
    process_ontology_nodes(graph, converter)

    # Connect to the database
    db_client = connect_to_destination_store(db_url, db_name)

    # Print the completion message
    print("Processing complete. Data inserted into the database.")


if __name__ == "__main__":
    main()
