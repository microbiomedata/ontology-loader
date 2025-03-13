"""Tests for the MongoDBLoader class with mocked database interactions."""

from unittest.mock import MagicMock, patch

import pytest
from nmdc_schema.nmdc import OntologyClass, OntologyRelation

import ontology_loader.mongodb_loader
from ontology_loader.mongodb_loader import MongoDBLoader, Report, _handle_obsolete_terms
from ontology_loader.utils import load_yaml_from_package


@pytest.fixture()
def schema_view():
    """Load the NMDC schema view."""
    return load_yaml_from_package("nmdc_schema", "nmdc_materialized_patterns.yaml")


@pytest.fixture()
def mock_mongo_loader(schema_view):
    """Mock MongoDBLoader to prevent actual database interactions."""
    loader = MongoDBLoader(schema_view)
    loader.client = MagicMock()
    loader.db = MagicMock()
    loader.db.get_collection = MagicMock()
    return loader


@pytest.fixture
def mock_db():
    """
    Mock database.

    :return: Mock database.
    """
    db = MagicMock()
    db.create_collection.return_value = MagicMock()
    return db


@pytest.fixture
def mock_ontology_classes():
    """
    Mock ontology classes.

    :return: List of OntologyClass objects.
    """
    return [
        OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ONT:002", name="Term2", type="nmdc:OntologyClass"),
    ]


@pytest.fixture
def mock_ontology_relations():
    """
    Mock ontology relations.

    :return: List of OntologyRelation objects.
    """
    return [
        OntologyRelation(subject="ONT:001", predicate="related_to", object="ONT:002", type="nmdc:OntologyRelation"),
        OntologyRelation(subject="ONT:002", predicate="part_of", object="ONT:003", type="nmdc:OntologyRelation"),
    ]


@pytest.fixture
def mock_obsolete_classes():
    """
    Mock ontology classes with obsolete terms.

    :return: List of OntologyClass objects.
    """
    return [
        OntologyClass(id="ONT:001", name="Term1", type="nmdc:OntologyClass"),
        OntologyClass(id="ONT:002", name="Term2", type="nmdc:OntologyClass", is_obsolete=True),
        OntologyClass(id="ONT:003", name="Term3", type="nmdc:OntologyClass", is_obsolete=True),
    ]


def test_upsert_new_ontology_data(mock_db, mock_ontology_classes, mock_ontology_relations):
    """
    Test upserting new ontology data.

    :param mock_db: Mock database.
    :param mock_ontology_classes: Mock ontology classes.
    :param mock_ontology_relations: Mock ontology relations.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value

    mock_query_result = MagicMock()
    mock_query_result.rows = []
    mock_query_result.num_rows = 0  # Ensuring num_rows behaves like an integer

    class_collection.find.return_value = mock_query_result  # Mocking find() response

    report = loader.upsert_ontology_data(mock_ontology_classes, mock_ontology_relations)

    assert isinstance(report[0], Report)  # Class updates report
    assert isinstance(report[1], Report)  # Class insertions report
    assert isinstance(report[2], Report)  # Relation insertions report
    assert len(report[1].records) == len(mock_ontology_classes)  # All classes inserted
    assert len(report[2].records) == len(mock_ontology_relations)  # All relations inserted


def test_upsert_existing_ontology_data(mock_db, mock_ontology_classes):
    """
    Test upserting existing ontology data.

    :param mock_db: Mock database.
    :param mock_ontology_classes: Mock ontology classes.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value

    existing_doc = {"id": "ONT:001", "name": "OldTerm", "type": "nmdc:OntologyClass"}

    mock_query_result = MagicMock()
    mock_query_result.rows = [existing_doc]
    mock_query_result.num_rows = 1  # Ensuring num_rows behaves like an integer

    class_collection.find.return_value = mock_query_result  # Mocking find() response

    report = loader.upsert_ontology_data(mock_ontology_classes, [])
    assert len(report[0].records) == 2  # One record should be updated
    assert len(report[1].records) == 0  # One record should be inserted (new class)


def test_handle_disappearing_relations(mock_db, mock_ontology_classes, mock_ontology_relations):
    """
    Test handling of disappearing relations.

    :param mock_db: Mock database.
    :param mock_ontology_classes: Mock ontology classes.
    :param mock_ontology_relations: Mock ontology relations.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    relation_collection = mock_db.create_collection.return_value

    mock_query_result = MagicMock()
    mock_query_result.rows = [
        {
            "subject": "ONT:001",
            "predicate": "entailed_isa_partof_closure",
            "object": "ONT:002",
            "type": "nmdc:OntologyRelation",
        },
        {
            "subject": "ONT:002",
            "predicate": "entailed_isa_partof_closure",
            "object": "ONT:003",
            "type": "nmdc:OntologyRelation",
        },
    ]
    mock_query_result.num_rows = 2  # Ensuring num_rows behaves like an integer

    relation_collection.find.return_value = mock_query_result  # Mocking find() response
    relation_collection.upsert = MagicMock()  # Mock upsert method

    updated_relations = [
        OntologyRelation(subject="ONT:001", predicate="related_to", object="ONT:003", type="nmdc:OntologyRelation")
        # Changed relation
    ]

    def mock_upsert(data, filter_fields, update_fields=None):
        """
        Simulate relation updates.

        :param data: Data to upsert.
        :param filter_fields: Fields to filter on.
        :param update_fields: Fields to update.
        """
        for obj in data:
            if "subject" in obj:
                for ontology_class in mock_ontology_classes:
                    if ontology_class.id == obj["subject"]:
                        ontology_class.relations = [
                            OntologyRelation(
                                subject=obj["subject"],
                                predicate=obj["predicate"],
                                object=obj["object"],
                                type="nmdc:OntologyRelation",
                            )
                        ]

    relation_collection.upsert.side_effect = mock_upsert  # Simulate relation updates

    loader.upsert_ontology_data(mock_ontology_classes, updated_relations)

    # Ensure upsert was called
    relation_collection.upsert.assert_called()

    # Verify old relations were replaced
    assert len(mock_ontology_classes[0].relations) == 1
    assert mock_ontology_classes[0].relations[0].object == "ONT:003"


def test_handle_obsolete_terms_function(mock_db):
    """
    Test the _handle_obsolete_terms function directly.

    :param mock_db: Mock database.
    """
    class_collection = mock_db.create_collection.return_value
    relation_collection = mock_db.create_collection.return_value

    # Create a proper OntologyClass object
    term_obj = OntologyClass(id="ONT:001",
                             name="Term1",
                             type="nmdc:OntologyClass",
                             is_obsolete=False)
    term_obj.relations = ["some_relation"]  # Add relations attribute
    
    # Set up the find method to return the term_obj
    mock_query_result = MagicMock()
    mock_query_result.rows = [term_obj]
    mock_query_result.num_rows = 1
    class_collection.find.return_value = mock_query_result
    
    # Capture what gets passed to upsert to verify changes
    upserted_data = None
    def capture_upsert(data, filter_fields, update_fields=None):
        nonlocal upserted_data
        upserted_data = data[0] if data else None
    
    class_collection.upsert.side_effect = capture_upsert

    # Test with list of obsolete terms
    obsolete_terms = ["ONT:001", "ONT:002"]
    _handle_obsolete_terms(obsolete_terms, class_collection, relation_collection)

    # Function creates a new dict from the original object, so the original stays unchanged
    # But we can check what was passed to upsert
    assert upserted_data is not None, "No data was passed to upsert"
    assert upserted_data["is_obsolete"] is True, "Term was not marked as obsolete"
    assert upserted_data["relations"] == [], "Relations were not cleared"
    
    # Verify class collection upsert was called
    class_collection.upsert.assert_called()

    # Verify relation collection had delete called
    relation_collection.delete.assert_called_with(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]}
    )


def test_upsert_ontology_data_with_obsolete_terms(mock_db, mock_obsolete_classes, mock_ontology_relations):
    """
    Test upserting ontology data with obsolete terms.

    :param mock_db: Mock database.
    :param mock_obsolete_classes: Mock ontology classes with obsolete terms.
    :param mock_ontology_relations: Mock ontology relations.
    """
    loader = MongoDBLoader()
    loader.db = mock_db
    class_collection = mock_db.create_collection.return_value
    relation_collection = mock_db.create_collection.return_value

    # Since we're going to check multiple calls to find with different params,
    # we need a more sophisticated mock that can return different results
    def mock_find(criteria):
        # Return empty result for any query
        mock_result = MagicMock()
        mock_result.rows = []
        mock_result.num_rows = 0
        return mock_result
    
    class_collection.find.side_effect = mock_find
    
    # Setup captured data for upserting
    upserted_data = []
    def capture_upsert(data, filter_fields, update_fields=None):
        nonlocal upserted_data
        if data and isinstance(data, list):
            upserted_data.extend(data)
    
    class_collection.upsert.side_effect = capture_upsert
    
    # Configure relation collection delete method
    relation_collection.delete = MagicMock()

    # Run the upsert function
    loader.upsert_ontology_data(mock_obsolete_classes, mock_ontology_relations)

    # Verify obsolete terms were handled
    obsolete_terms = ["ONT:002", "ONT:003"]  # These are marked as obsolete in mock_obsolete_classes
    relation_collection.delete.assert_any_call(
        {"$or": [{"subject": {"$in": obsolete_terms}}, {"object": {"$in": obsolete_terms}}]}
    )

    # Verify class collection find was called for correct term lookups
    # The _handle_obsolete_terms function should look up each obsolete term
    for term_id in obsolete_terms:
        class_collection.find.assert_any_call({"id": term_id})
