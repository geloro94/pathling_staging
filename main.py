import pandas as pd
import json
import os
import subprocess
from collections import defaultdict
from typing import List, Dict
from io import StringIO

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import requests

from PathlingViewDefinitionRunner import run_view_definition, ViewDefinition, get_column_names

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})

FHIR_SERVER_BASE_URL = "https://hapi.fhir.org/baseR4"
PATHLING_CONTAINER_NAME = "pathling-data-extraction-server-1"
PATHLING_BEARER_TOKEN = "f4a41f0d-fb75-45b5-88a7-461d4ad95f11"
PATHLING_BASE_URL = os.environ.get("PATHLING_BASE_URL", "http://localhost:8093/fhir")
STAGING_DIR = "/usr/share/staging"

SUPPORTED_RESOURCE_TYPES = ["Patient", "Observation", "Condition", "Consent", "Procedure", "MedicationAdministration",
                            "MedicationStatement", "Specimen"]

CODE_PARAMETER_BY_RESOURCE_TYPE = {
    "Observation": "code",
    "Condition": "code",
    "Procedure": "code",
    "MedicationAdministration": "medication.code",
    "MedicationStatement": "medicine.code",
    "Specimen": "type"
}

DATE_PARAMETER_BY_RESOURCE_TYPE = {
    "Observation": "effective",
    "Condition": "onset",
    "Procedure": "performed",
    "MedicationAdministration": "effective",
    "MedicationStatement": "effective",
    "Specimen": "collected"
}


@app.route("/run_extraction", methods=["POST"])
def run_extraction():
    view_definitions = json.loads(request.get_data())
    merged_data = pd.DataFrame()
    for definition in view_definitions:
        view_definition = ViewDefinition.from_json(json.dumps(definition))
        print(json.dumps(definition))
        result = run_view_definition(view_definition, "http://localhost:8093/fhir", 60)

        column_names = get_column_names(view_definition)

        result_df = pd.read_csv(StringIO(result.text), names=column_names)

        if merged_data.empty:
            merged_data = result_df
        else:
            merged_data['Patient id'] = merged_data['Patient id'].astype(str)
            result_df['Patient id'] = result_df['Patient id'].astype(str)
            merged_data = pd.merge(merged_data, result_df, "outer", on='Patient id')

    csv_output = merged_data.to_csv()

    response = Response(csv_output, mimetype='text/csv')
    response.headers['Content-Disposition'] = 'attachment; filename=extracted_data.csv'

    return response


@app.route("/etl_everything", methods=["POST"])
def etl_everything_endpoint():
    data = request.get_json()
    patient_ids = data["patient_ids"]

    response_bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": []
    }

    for patient_id in patient_ids:
        # Initialize the search URL for the current patient
        next_url = f"{FHIR_SERVER_BASE_URL}/Patient/{patient_id}/$everything"

        while next_url:
            # Make the request to the FHIR server
            search_response = requests.get(next_url)
            search_response.raise_for_status()
            search_results = search_response.json()

            # Extend the response bundle with the current page of results
            response_bundle["entry"].extend(search_results.get("entry", []))

            # Check for a 'next' link to continue paging
            next_link = [link for link in search_results.get("link", []) if link.get("relation") == "next"]
            next_url = next_link[0].get("url") if next_link else None

    response = process_and_import_fhir_bundle(response_bundle)

    return response.json(), response.status_code


@app.route("/etl", methods=["POST"])
def etl_endpoint():
    data = request.get_json()

    patient_ids = data["patient_ids"]
    resource_requests = data["resource_requests"]

    response_bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": []
    }

    for patient_id in patient_ids:
        for resource_request in resource_requests:
            resource_type = resource_request["resource_type"]
            if resource_type not in SUPPORTED_RESOURCE_TYPES:
                return jsonify({"error": f"Unsupported resource type {resource_type}"}), 400

            search_url = f"{FHIR_SERVER_BASE_URL}/{resource_type}?patient=Patient/{patient_id}"

            if "code" in resource_request:
                code = resource_request["code"]
                code_parameter = CODE_PARAMETER_BY_RESOURCE_TYPE[resource_type]
                search_url += f"&{code_parameter}={code}"

            date_parameter = DATE_PARAMETER_BY_RESOURCE_TYPE.get(resource_type)
            if "date_start" in resource_request and "date_end" in resource_request:
                date_start = resource_request.get("date_start")
                date_end = resource_request.get("date_end")
                if date_start:
                    search_url += f"&{date_parameter}=ge{date_start}"
                if date_end:
                    search_url += f"&{date_parameter}=le{date_end}"

            if "max_results" in resource_request:
                max_results = resource_request["max_results"]
                search_url += f"&_count={max_results}"
            if resource_type == "Observation":
                search_url += f"&sort={date_parameter}"

            search_response = requests.get(search_url)
            search_response.raise_for_status()
            search_results = search_response.json()

            response_bundle["entry"].extend(search_results.get("entry", []))
        search_url = f"{FHIR_SERVER_BASE_URL}/Patient/{patient_id}"
        search_response = requests.get(search_url)
        if search_response.status_code == 200:
            search_results = search_response.json()
            response_bundle["entry"].append({"resource": search_results})

    response = process_and_import_fhir_bundle(response_bundle)

    return response.json(), response.status_code


def write_ndjson_by_resource_type(resources: List[dict], filename: str) -> Dict[str, str]:
    resources_by_type = defaultdict(list)
    for resource in resources:
        resource_type = resource['resourceType']
        print(resource_type)
        resources_by_type[resource_type].append(resource)

    file_name_by_type = {}
    for resource_type, type_resources in resources_by_type.items():
        type_filename = f"{filename}-{resource_type}.ndjson"
        file_name_by_type[resource_type] = type_filename
        write_ndjson(type_resources, "pathling/data/ndjson/" + type_filename)

    return file_name_by_type


def write_ndjson(resources: List[dict], filename: str):
    with open(filename, 'w') as file:
        for resource in resources:
            file.write(json.dumps(resource) + '\n')


def create_parameters(file_name_by_type: Dict[str, str], staging_dir, mode: str = None) \
        -> Dict[str, List[Dict[str, str]]]:
    parameter_list = []

    for resource_type, type_filename in file_name_by_type.items():
        file_url = f"file://{staging_dir}/{type_filename}"
        source_parts = [
            {"name": "resourceType", "valueCode": resource_type},
            {"name": "url", "valueUrl": file_url}
        ]

        if mode:
            source_parts.append({"name": "mode", "valueCode": mode})

        parameter_list.append({"name": "source", "part": source_parts})

    parameters = {"resourceType": "Parameters", "parameter": parameter_list}
    return parameters


def copy_file_to_container(src_file: str, container_name: str, destination_path: str) -> None:
    try:
        subprocess.run(
            ["docker", "cp", src_file, f"{container_name}:{destination_path}"],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error copying file: {e}")
        raise


def import_files_to_pathling(parameters, fhir_endpoint, bearer_token):
    headers = {
        "Content-Type": "application/fhir+json",
        "Accept": "application/fhir+json",
        "Authorization": f"Bearer {bearer_token}"
    }
    response = requests.post(f"{fhir_endpoint}/$import", headers=headers, json=parameters)
    print(parameters)

    if response.status_code != 200:
        print(f"Error: {response.status_code}, {response.text}")
    else:
        print("Import successful")
    return response


def process_and_import_fhir_bundle(bundle: dict):
    resources = [entry.get("resource") for entry in bundle["entry"] if entry.get("resource")]
    if not resources:
        return jsonify({"error": "No resources found"}), 404
    # Generate NDJSON files from the FHIR Bundle
    file_name_by_type = write_ndjson_by_resource_type(resources, "example")

    # Copy the generated files to the Docker container
    # for resource_type, file_name in file_name_by_type.items():
    #     copy_file_to_container(file_name, PATHLING_CONTAINER_NAME, STAGING_DIR)

    # Generate the parameters for the $import request
    parameters = create_parameters(file_name_by_type, STAGING_DIR)

    # Send the import request to the Pathling server
    return import_files_to_pathling(parameters, PATHLING_BASE_URL, PATHLING_BEARER_TOKEN)


if __name__ == "__main__":
    app.run(debug=True)
