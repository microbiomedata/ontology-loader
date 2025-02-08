from oaklib import get_adapter

def process_ontology(ontology: str):
    # Load the ontology graph
    adapter = get_adapter("sqlite:obo:"+ontology)
    adapter.precompute_lookups()
    for entity in adapter.entities():
        if entity.startswith("ENVO:"):
            print(list(adapter.ancestors(entity, reflexive=True, predicates=["rdfs:subClassOf", "BFO:0000050"])))

if __name__ == "__main__":
    process_ontology("envo")

