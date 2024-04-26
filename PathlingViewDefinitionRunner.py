# View Defintion Runner on top of Pathling Extract API
from datetime import datetime
from enum import Enum

import requests
import json
from typing import List

VIEW_DEFINITION_RESOURCE_TYPE = "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition"


class Parameter:
    def __init__(self, name: str, value_string: str):
        self.name = name
        self.valueString = value_string


class ColumnParameter(Parameter):
    def __init__(self, value_string: str):
        super().__init__("column", value_string)


class FilterParameter(Parameter):
    def __init__(self, value_string: str):
        super().__init__("filter", value_string)


class LimitParameter(Parameter):
    def __init__(self, value_string: str):
        super().__init__("limit", value_string)


class Parameters:
    def __init__(self):
        self.resourceType = "Parameters"
        self.parameter: List[Parameter] = []

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)


class Status(Enum):
    Draft = "draft"
    Active = "active"
    Retired = "retired"
    Unknown = "unknown"


class ColumnBackBoneElement:
    """
    @param name: The name of the column to be selected
    @param path: The FHIRPath to extract the value from the resource for the column
    """

    def __init__(self, name: str, path: str):
        self.name = name
        self.path = path


class SelectBackBoneElement:
    """
    @param column: column definitions of the select clause of the view definition
    """

    def __init__(self, column: List[ColumnBackBoneElement]):
        self.column: List[ColumnBackBoneElement] = column


class WhereBackBoneElement:
    """
    @param path: Filter value for the where clause of the view definition as FHIRPath
    """

    def __init__(self, path: str):
        self.path: str = path


class ViewDefinition:
    """
    Simple class to represent the Logical Model ViewDefintion see https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html
    Focusing on required fields and fields required for this implementation

    @param resource: The name of the target resource of the view definition
    """

    def __init__(self, resource: str, resourceType: str = VIEW_DEFINITION_RESOURCE_TYPE,
                 status: str = "active",
                 fhirVersion: str = "4.0.1", date=datetime.now().isoformat(), select=None,
                 where=None, name: str = None):
        if select is None:
            select = []
        if where is None:
            where = []
        self.resourceType = resourceType
        self.resource = resource
        self.status = status
        self.date = date
        self.fhirVersion = fhirVersion
        self.select: List[SelectBackBoneElement] = select
        self.where: List[WhereBackBoneElement] = where
        self.name: str = name

    @classmethod
    def from_json(cls, json_string):
        json_dict = json.loads(json_string)
        print(json_dict)
        view_definition = cls(resource=json_dict["resource"], resourceType=json_dict["resourceType"],
                              status=json_dict["status"], fhirVersion=json_dict["fhirVersion"],
                              date=json_dict["date"], name=json_dict["name"])
        view_definition.select = [
            SelectBackBoneElement(
                column=[ColumnBackBoneElement(name=column["name"], path=column["path"]) for column in select["column"]])
            for select in
            json_dict["select"]]
        view_definition.where = [WhereBackBoneElement(path=where["path"]) for where in json_dict.get("where", [])]
        return view_definition


def run_extraction_query(resource_type, parameters: Parameters, fhir_server_base_url, timeout=60):
    headers = {
        "Content-Type": "application/fhir+json"
    }
    print(parameters.to_json())
    response = requests.post(url=f"{fhir_server_base_url}/{resource_type}/$extract",
                             data=parameters.to_json(),
                             headers=headers,
                             timeout=timeout)
    return response.json()


def poll_extraction_job(result_url, timeout=60):
    response = requests.get(result_url, timeout=timeout)
    return response


def run_view_definition(view_definition, fhir_server_base_url, timeout=60):
    parameters = Parameters()
    for select in view_definition.select:
        for column in select.column:
            parameters.parameter.append(ColumnParameter(column.path))
    for where in view_definition.where:
        parameters.parameter.append(FilterParameter(where.path))

    response = run_extraction_query(view_definition.resource, parameters, fhir_server_base_url, timeout)
    print(response)

    result_url = None

    for param in response.get("parameter", []):  # Use .get to avoid KeyError if 'parameter' is missing
        if param.get("name") == "url":  # Check if this parameter has the name 'url'
            result_url = param.get("valueUrl")  # Extract the 'valueUrl'
            break  # Exit the loop once the URL is found
    if not result_url:
        print("URL not found in the response.")
    return poll_extraction_job(result_url, timeout)


def get_column_names(view_definition: ViewDefinition):
    column_names = []
    for select in view_definition.select:
        for column in select.column:
            column_names.append(column.name)
    return column_names


if __name__ == "__main__":
    # definition = ViewDefinition.from_json("""{
    # "resource": "Patient",
    # "date": "2024-02-27T14:20:18.732269",
    # "fhirVersion": "4.0.1",
    # "resourceType": "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition",
    # "name": "PatientView",
    # "select": [
    #     {
    #         "column": [
    #             {
    #                 "name": "patientId",
    #                 "path": "Patient.id"
    #             },
    #             {
    #                 "name": "patientName",
    #                 "path": "Patient.name.family.first()"
    #             },
    #             {
    #                 "name": "patientGivenName",
    #                 "path": "Patient.name.given.first()"
    #             },
    #             {
    #                 "name": "MetaLastUpdated",
    #                 "path": "Patient.meta.lastUpdated"
    #             }
    #         ]
    #     }
    # ],
    # "status": "active"
    # }""")
    # get_column_names(definition)
    # result = run_view_definition(definition, "http://localhost:8093/fhir", 60)
    # print(result.text)
    definition2 = ViewDefinition.from_json("""{
    "resource": "Composition",
    "date": "2024-02-27T14:20:18.732269",
    "fhirVersion": "4.0.1",
    "resourceType": "http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition",
    "name": "PatientView",
    "select": [
        {
            "column": [
                {
                    "name": "patientName",
                    "path": "Composition.subject.resolve().ofType(Patient).name.family.first()"
                }
            ]
        }
    ],
    "status": "active"
    }""")
    print(get_column_names(definition2))
    result = run_view_definition(definition2, "http://localhost:8093/fhir", 60)
    print(result.text)