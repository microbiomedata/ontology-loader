from oaklib import get_adapter

def process_ontology(ontology: str):
    # Load the ontology graph
    adapter = get_adapter("sqlite:obo:"+ontology)
    adapter.precompute_lookups()
    for entity in adapter.entities():
        if entity.startswith("ENVO:"):
            # Convert generator to list
            ancestors_list = list(adapter.ancestors(entity, reflexive=True, predicates=["rdfs:subClassOf", "BFO:0000050"]))

            # Only print if the list contains terms that start with "ENVO:"
            filtered_ancestors = [a for a in ancestors_list if a.startswith("ENVO:")]

            if filtered_ancestors:  # Print only if the list is non-empty
                print(f"{entity} -> {filtered_ancestors}")

if __name__ == "__main__":
    process_ontology("envo")

