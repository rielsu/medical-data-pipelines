# transformation/tests/test_flattener.py
import pytest
from flattener import snake_case, flatten_json


def test_snake_case():
    assert snake_case("TestString") == "test_string"
    assert snake_case("testString") == "test_string"
    assert (
        snake_case("TestStringWithMultipleCAPS") == "test_string_with_multiple_c_a_p_s"
    )
    assert snake_case("test") == "test"
    assert snake_case("") == ""


def test_flatten_json():
    nested_json = {
        "Name": "John",
        "Location": {"City": "New York", "State": "NY"},
        "Details": [{"Age": 30}, {"Age": 33}],
    }
    expected_flat_json = {
        "name": "John",
        "location__city": "New York",
        "location__state": "NY",
        "details__age": [30, 33],
    }

    assert flatten_json(nested_json) == expected_flat_json


def test_empty_json():
    # Test empty JSON
    assert flatten_json({}) == {}


def test_not_nested_json():
    # Test non-nested JSON
    non_nested_json = {"Name": "Jane", "Age": 28}
    expected_non_nested_json = {"name": "Jane", "age": 28}
    assert flatten_json(non_nested_json) == expected_non_nested_json


def test_flatten_immunization_record():
    input_json = {
        "date": "2022-11-27",
        "identifier": [
            {
                "system": "https:fhir.kp.org",
                "value": "TUxNoFZwXTHqn.VhBSrFeT2x1-3Q.EhjEJnqDi5P8i40B",
            }
        ],
        "wasNotGiven": False,
        "meta": {
            "lastUpdated": "2023-06-26T18:03:07.062Z",
            "versionId": "9000000000000",
        },
        "patient": {"reference": "Patient/62", "display": "P, M"},
        "reported": True,
        "id": "62fdab4811870fe0d37b2368ff4ebe2b2cfd5015b52cc6b2",
        "vaccineCode": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/sid/cvx",
                    "code": "88",
                    "display": "INF (INFLUENZA) UNSPECIFIED FORMULATION",
                }
            ],
            "text": "INF (Influenza) unspecified formulation",
        },
        "resourceType": "Immunization",
        "status": "completed",
    }

    expected_output = {
        "date": "2022-11-27",
        "identifier__system": ["https:fhir.kp.org"],
        "identifier__value": ["TUxNoFZwXTHqn.VhBSrFeT2x1-3Q.EhjEJnqDi5P8i40B"],
        "was_not_given": False,
        "meta__last_updated": "2023-06-26T18:03:07.062Z",
        "meta__version_id": "9000000000000",
        "patient__reference": "Patient/62",
        "patient__display": "P, M",
        "reported": True,
        "id": "62fdab4811870fe0d37b2368ff4ebe2b2cfd5015b52cc6b2",
        "vaccine_code__coding__system": ["http://hl7.org/fhir/sid/cvx"],
        "vaccine_code__coding__code": ["88"],
        "vaccine_code__coding__display": ["INF (INFLUENZA) UNSPECIFIED FORMULATION"],
        "vaccine_code__text": "INF (Influenza) unspecified formulation",
        "resource_type": "Immunization",
        "status": "completed",
    }

    assert flatten_json(input_json) == expected_output
