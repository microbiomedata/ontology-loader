## ontology_loader

Suite of tools to configure and load an ontology from the OboFoundary into the data object for OntologyClass as 
specified by NMDC schema.

## Development Environment

#### Pre-requisites

- \>=Python 3.8
- Poetry
- Docker
- MongoDB
- NMDC materialized schema

```bash

% docker pull mongo
% docker run -d --name mongodb-container -p 27017:27017 mongo
```

#### Basic mongosh commands
```bash
% docker ps
% docker exec -it [mongodb-container-id] bash
% mongosh
% show dbs
% use test
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
