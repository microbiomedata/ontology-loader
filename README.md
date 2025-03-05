## ontology_loader

Suite of tools to configure and load an ontology from the OboFoundary into the data object for OntologyClass as 
specified by NMDC schema.

## Development Environment

#### Pre-requisites

- >=Python 3.9
- Poetry
- Docker
- MongoDB
- NMDC materialized schema
- ENV variable for MONGO_PASSWORD (or pass it in via the cli/runner itself directly)

```bash

% docker pull mongo
% docker run -d --name mongodb-container -p 27018:27017 mongo
```

#### Basic mongosh commands
```bash
% docker ps
% docker exec -it [mongodb-container-id] bash
% mongosh mongodb://admin:root@mongo:27017/nmdc?authSource=admin
% show dbs
% use nmdc
% db.ontology_class_set.find().pretty()
% db.ontology_relation_set.find().pretty()
``` 

#### Command line
```bash
% poetry install
% poetry run ontology_loader --help
% poetry run ontology_loader --source-ontology "envo"
% poetry run ontology_loader --source-ontology "go"
```

#### Running the tests
```bash
% make test
```

#### Running the linter
```bash
% make lint
```

#### python example usage
```bash
pip install nmdc-ontology-loader
```

```python
from  nmdc_ontology_loader.ontology_loader import OntologyLoader
import tempfile

def test_load_ontology():
    """Test the load_ontology method."""
    ontology_loader = OntologyLoader(
        source_ontology="envo",
        output_directory=tempfile.gettempdir(),
        generate_reports=True,
    )
    ontology_loader.load_ontology()
    assert ontology_loader.ontology_class_set
    assert ontology_loader.ontology_relation_set
    assert ontology_loader.ontology_class_set.count() > 0
    assert ontology_loader.ontology_relation_set.count() > 0
```

### Reset collections in dev

```
% docker exec -it mongodb-container bash
% mongosh mongodb://admin:root@mongo:27017/nmdc?authSource=admin
% db.ontology_class_set.drop()
% db.ontology_relation_set.drop()
```