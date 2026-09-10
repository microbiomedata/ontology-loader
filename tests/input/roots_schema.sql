-- Minimal semsql schema for root relationship fixtures.
CREATE TABLE statements (
	stanza TEXT,
	subject TEXT,
	predicate TEXT,
	object TEXT,
	value TEXT,
	datatype TEXT,
	language TEXT
, graph TEXT);

CREATE VIEW rdf_type_statement AS SELECT * FROM statements WHERE predicate='rdf:type';

CREATE VIEW rdfs_subclass_of_statement AS SELECT * FROM statements WHERE predicate='rdfs:subClassOf';

CREATE VIEW class_node AS SELECT distinct subject AS id from rdf_type_statement WHERE object = 'owl:Class';

CREATE VIEW object_property_node AS SELECT DISTINCT subject AS id FROM rdf_type_statement WHERE object='owl:ObjectProperty';

CREATE VIEW owl_has_value AS SELECT onProperty.subject AS id,
            onProperty.object AS on_property,
            f.object AS filler
            FROM
       statements AS onProperty,
       statements AS f
     WHERE
       onProperty.predicate = 'owl:onProperty' AND
       onProperty.subject=f.subject AND
       f.predicate='owl:hasValue';

CREATE VIEW owl_some_values_from AS SELECT onProperty.subject AS id,
            onProperty.object AS on_property,
            f.object AS filler
            FROM
       statements AS onProperty,
       statements AS f
     WHERE
       onProperty.predicate = 'owl:onProperty' AND
       onProperty.subject=f.subject AND
       f.predicate='owl:someValuesFrom';

CREATE VIEW owl_subclass_of_some_values_from AS SELECT subClassOf.stanza,
         subClassOf.subject,
         svf.on_property AS predicate,
         svf.filler AS object
         FROM
    statements AS subClassOf,
    owl_some_values_from AS svf
  WHERE
    subClassOf.predicate = 'rdfs:subClassOf' AND
    svf.id=subClassOf.object;

CREATE VIEW rdfs_subclass_of_named_statement AS SELECT * FROM rdfs_subclass_of_statement WHERE object NOT LIKE '_:%';

CREATE VIEW rdfs_subproperty_of_statement AS SELECT * FROM statements WHERE predicate='rdfs:subPropertyOf';

CREATE VIEW edge AS SELECT subject, predicate, object
  FROM owl_subclass_of_some_values_from
   UNION
  SELECT subject, predicate, object
  FROM rdfs_subclass_of_named_statement
   UNION
  SELECT subject, predicate, object
   FROM rdfs_subproperty_of_statement
   UNION
  SELECT subject, predicate, object
  FROM rdf_type_statement WHERE object IN (SELECT id FROM class_node);

CREATE VIEW deprecated_node AS SELECT DISTINCT subject AS id FROM statements WHERE predicate='owl:deprecated' AND value='true';

CREATE VIEW node AS SELECT subject AS id FROM statements UNION SELECT object AS id FROM statements WHERE object IS NOT NULL;
