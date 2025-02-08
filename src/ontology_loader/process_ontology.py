from oaklib import get_adapter

def get_ontology_terms_and_metadata(ontology: str):
    # Load the ontology graph
    adapter = get_adapter("sqlite:obo:"+ontology)
    adapter.precompute_lookups()
    for entity in adapter.entities():
        if entity.startswith("ENVO:"):
            print(entity)
            print(adapter.aliases(entity))
            print(adapter.definition(entity))
            print(adapter.xrefs(entity))

def get_ontology_relations_closure(ontology: str, predicates: list = None):
    # Load the ontology graph
    predicates = ["rdfs:subClassOf", "BFO:0000050"] if predicates is None else predicates
    adapter = get_adapter("sqlite:obo:"+ontology)
    adapter.precompute_lookups()
    for entity in adapter.entities():
        if entity.startswith("ENVO:"):
            # Convert generator to list
            ancestors_list = list(adapter.ancestors(entity, reflexive=True, predicates=predicates))

            # Only print if the list contains terms that start with "ENVO:"
            filtered_ancestors = list(set([a for a in ancestors_list if a.startswith("ENVO:")]))

            if filtered_ancestors:  # Print only if the list is non-empty
                print(f"{entity} -> {filtered_ancestors}")

if __name__ == "__main__":
    get_ontology_terms_and_metadata("envo")

