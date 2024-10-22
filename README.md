## ontology_loader

Suite of tools to configure and load an ontology from the OboFoundary into the data object for OntologyClass as specified by NMDC schema

## Acknowledgements

This [cookiecutter](https://cookiecutter.readthedocs.io/en/stable/README.html) project was developed from the 
[nmdc-project-template](https://github.com/sierra-moxon/nmdc-project-template) template and will be kept 
up-to-date using [cruft](https://cruft.github.io/cruft/).


## Development Environment

#### Pre-requisites
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
``` 

#### Command line
```bash
% poetry install
% poetry run ontology_loader --help
% poetry run ontology_loader --source-ontology-url "https://purl.obolibrary.org/obo/envo.json"
% poetry run ontology_loader --source-ontology-url "https://purl.obolibrary.org/obo/go.json"
```
