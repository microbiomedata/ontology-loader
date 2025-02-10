import importlib.resources
import yaml  # Ensure PyYAML is installed
from linkml_runtime.utils.schemaview import SchemaView

def load_yaml_from_package(package: str, filename: str) -> SchemaView:
    """
    Loads a YAML file from a given package.

    :param package: The package where the YAML file is located (e.g., "nmdc_schema").
    :param filename: The YAML file to load (e.g., "nmdc_materialized_patterns.yaml").
    :return: Parsed YAML data as a Python dictionary.
    """
    with importlib.resources.files(package).joinpath(filename).open("r") as f:
        return SchemaView(f)
