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

#### MongoDB Connection Settings

When connecting to MongoDB, you need to set the correct environment variables depending on where your code is running:

1. When running from your local machine (CLI or tests):
   ```bash
   export MONGO_HOST=localhost
   export MONGO_PORT=27018
   export ENABLE_DB_TESTS=true
   export MONGO_PASSWORD="your_valid_password"
   ```

2. When running inside Docker containers:
   ```bash
   export MONGO_HOST=mongo
   export MONGO_PORT=27017
   ```

The Docker container networking uses container names (like 'mongo') for internal communication, while your host machine must use 'localhost' with the mapped port (27018).

#### Basic mongosh commands
```bash
% docker ps
% docker exec -it [mongodb-container-id] bash
% mongosh mongodb://admin:root@mongo:27017/nmdc?authSource=admin
% show dbs
% use nmdc
% db.ontology_class_set.find().pretty()
% db.ontology_relation_set.find().pretty()
% db.ontology_class_set.find( { id: { $regex: /^PO/ } } ).pretty()
% db.ontology_class_set.find( { id: { $regex: /^UBERON/ } } ).pretty()
% db.ontology_class_set.find( { id: { $regex: /^ENVO/ } } ).pretty()
% db.ontology_class_set.find( { alternative_identifiers: { $exists: true, $ne: [] } } ).pretty()
``` 

#### Command line
```bash
% poetry install
% poetry run ontology_loader --help
% poetry run ontology_loader --source-ontology "envo"
% poetry run ontology_loader --source-ontology "uberon"
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

### Testing CRUD operations in a live MongoDB

If you want to test the CRUD operations in a live MongoDB instance, you need to set two environment variables:
MONGO_PASSWORD="your_valid_password"
ENABLE_DB_TESTS=true

This will allow you to run tests to actually insert/update/delete records in your MongoDB tests instance instead
of simply mocking the calls. You can then run the tests with the following command:

```bash
make test
```
 
The same test command will run without the environment variables, but it will only mock the calls to the database.
This is intended to help prevent accidental data loss or corruption in a live database environment and to 
ensure that MONGO_PASSWORD is not hardcoded in the codebase.

### Reset collections in dev

```bash
docker exec -it nmdc-runtime-test-mongo-1 bash
```
```bash
mongosh mongodb://admin:root@mongo:27017/nmdc?authSource=admin
db.ontology_class_set.find({}).pretty()
db.ontology_relation_set.find({}).pretty()
db.biosample_set.find({}).pretty()
db.ontology_class_set.drop()
db.ontology_relation_set.drop()
db.ontology_class_set.countDocuments()
db.ontology_relation_set.countDocuments()
```