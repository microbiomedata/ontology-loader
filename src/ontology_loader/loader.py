"""load ontology"""
import logging
from pathlib import Path
from oaklib import get_implementation_from_shorthand
from oaklib.interfaces import OboGraphInterface
from oaklib.datamodels.obograph import GraphDocument, Graph
import yaml
from oaklib import get_implementation_from_shorthand
from nmdc_schema.nmdc import OntologyClass
from pymongo import MongoClient
from oaklib import get_adapter
from oaklib.query import onto_query, SimpleQueryTerm

# Configure logging
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(file_name: str) -> dict:
    """
    Load a YAML configuration file from the src directory

    :param file_name: The name of the YAML file to load
    :return: The configuration as a dictionary
    """
    # Get the directory of the current file
    current_file_dir = Path(__file__).resolve().parent

    # Construct the path to the src directory one level above
    config_path = current_file_dir.parent / file_name

    # Open and load the YAML file
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Function to fetch metadata based on config
def fetch_metadata(oi, curie, metadata_config):
    """
    Fetch metadata for a given ontology term based on the configuration

    :param oi: The ontology implementation
    :param curie: The CURIE of the ontology term
    :param metadata_config: The metadata configuration
    :return: A dictionary of metadata
    """
    metadata = {}

    if 'synonyms' in metadata_config:
        synonyms = oi.synonyms(curie)
        metadata['synonyms'] = [synonym.val for synonym in synonyms] if synonyms else []

    if 'definitions' in metadata_config:
        definition = oi.definition(curie)
        metadata['description'] = definition if definition else "No definition available"

    if 'xrefs' in metadata_config:
        xrefs = oi.xrefs(curie)
        metadata['xrefs'] = xrefs if xrefs else []

    return metadata

# Function to process ontologies as per the config
def process_ontology(metadata_config, adapter):
    """
    Process an ontology based on the configuration

    :param source: The source of the ontology
    :param metadata_config: The metadata configuration
    :return: None
    """

    # Iterate over all terms in the ontology
    for curie, label in adapter.entities(owl_type='class'):
        # Fetch metadata (definitions, synonyms, etc.)
        metadata = fetch_metadata(adapter, curie, metadata_config)

        # Create NMDC OntologyClass object
        ontology_class = OntologyClass(
            id=curie,
            name=label,
            **metadata
        )

        # Example print (or insert into MongoDB)
        print(ontology_class.json())  # Replace with MongoDB insertion logic

# Function to process all ontologies from config
def process_all_ontologies(ontology_config, oak_config):
    """
    Process all ontologies based on the configuration

    :param ontology_config: The ontologies to load configuration dictionary
    :param oak_config: The OAK adapter configuration dictionary
    :return: None
    """
    current_file_dir = Path(__file__).resolve().parent

    # Construct the path to the file in the parent directory
    oak_config_file = current_file_dir.parent / oak_config

    # Initialize an OAK implementation for the OBO Graph format
    ontology_source = "sqlite:obo:envo"  # OAK shorthand for the ENVO ontology
    oi: OboGraphInterface = get_adapter(ontology_source)

    graph = oi.as_obograph()

    for node in graph.nodes:
        if node.id.startswith("ENVO"):
            print(node.id)
            print(node.lbl)
            if node.meta.definition is not None:
                print(node.meta.definition.val)
            if node.meta.xrefs is not None and node.meta.xrefs != []:
                for xref in node.meta.xrefs:
                    print(xref.val)
            if node.meta.synonyms is not None and node.meta.synonyms != []:
                for synonym in node.meta.synonyms:
                    print(synonym.val)

    # for edge in graph.edges:
    #     print(edge)


    # You can also filter terms based on specific predicates like 'is_a' or 'part_of'

# Load configuration and process
ontology_config = load_config("ontology-config.yaml")
print(ontology_config)
process_all_ontologies(ontology_config, "oak-config.yaml")


# client = MongoClient('mongodb://localhost:27017/')
# db = client['ontology_db']
# collection = db['terms']
#
# # Insert into MongoDB
# collection.insert_one(ontology_class.dict())