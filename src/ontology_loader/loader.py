"""load ontology"""
import logging
from linkml_store.api.config import ClientConfig
from prefixmaps import load_context
from curies import Converter
from nmdc_schema.nmdc import OntologyClass
from oaklib import get_adapter
from pprint import pprint
from oaklib.interfaces import OboGraphInterface
from linkml_store import Client
import importlib.resources

def find_schema():
    # Use importlib.resources to get the path of the package's YAML file
    yaml_file_path = importlib.resources.files('nmdc_schema').joinpath('nmdc_materialized_patterns.yaml')
    return str(yaml_file_path)

logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')


def _extract_synonyms(node):
    """Extract alternative names from node's synonyms."""
    synonyms = node.meta.synonyms if node.meta else []
    return [synonym.val for synonym in synonyms] if synonyms else []


def _extract_description(node):
    """Extract description from node's definition."""
    return node.meta.definition.val if node.meta and node.meta.definition else "No definition available"


def _compress_xref(xref_val, converter):
    """Helper to compress xrefs, including handling for https to http conversion."""
    curie = converter.compress(str(xref_val))
    if curie and curie.startswith('https'):
        print(curie)
        curie = converter.compress(curie.replace('http', 'https'))
    return curie


def _extract_xrefs(node, cmaps):
    """Extract and contract xrefs using the provided prefix map."""
    xrefs = node.meta.xrefs if node.meta else []
    converter = Converter.from_prefix_map(cmaps, strict=False)
    contracted_xrefs = []

    for xref in xrefs:
        if not xref.val:
            continue
        curie = _compress_xref(xref.val, converter)
        contracted_xrefs.append(curie or xref.val)

    return contracted_xrefs


def fetch_metadata(node, cmaps):
    """
    Fetch metadata for a given ontology term based on the configuration.

    :param node: The ontology node
    :param cmaps: The prefix_converter from prefixmaps using the "merged" context
    :return: A dictionary of metadata
    """
    metadata = {
        'alternative_names': _extract_synonyms(node),
        'description': _extract_description(node),
        'alternative_identifiers': _extract_xrefs(node, cmaps)
    }
    return metadata


class OntologyProcessor:
    def __init__(self, ontology="envo"):
        self.client_config = None
        self.ontology = ontology
        self.db = None
        self.graph = None
        self.cmaps = None

    def connect_to_destination_store(self,
                                     db_url="mongodb://localhost:27017",
                                     db_name="test"):
        # Initialize MongoDB using LinkML-store's client
        self.db = Client()
        self.client_config = ClientConfig(handle=db_url, schema_path=find_schema())
        self.db.attach_database(db_url, db_name)

    def initialize_oak(self):
        # Initialize an OAK to download the ontology
        ontology_source = f"sqlite:obo:{self.ontology}"
        oi: OboGraphInterface = get_adapter(ontology_source)
        self.graph = oi.as_obograph()

    def initialize_prefix_map(self):
        # Initialize prefix map to contract URIs in xrefs to curies
        context = load_context("merged")
        extended_prefix_map = context.as_extended_prefix_map()
        converter = Converter.from_extended_prefix_map(extended_prefix_map)
        self.cmaps = converter.prefix_map

    def process_ontology_nodes(self):
        # Process ontology nodes and insert them into the database
        ontology_classes = []
        for node in self.graph.nodes:
            if node.id.startswith("ENVO"):
                metadata = fetch_metadata(node, self.cmaps)
                pprint(metadata)
                # Create an NMDC OntologyClass object
                ontology_class = OntologyClass(
                    id=node.id,
                    name=node.lbl,
                    type="nmdc:OntologyClass",
                    **metadata
                )
                ontology_classes.append(ontology_class)

        self.db.create_collection("OntologyClass", recreate_if_exists=True).insert(ontology_classes)

    def process(self):
        self.initialize_prefix_map()
        self.initialize_oak()
        self.connect_to_destination_store()
        self.process_ontology_nodes()


# Example usage
print("Processing ENVO ontology")
print("find the NMDC schema file", find_schema())
processor = OntologyProcessor(ontology="envo")
processor.process()
