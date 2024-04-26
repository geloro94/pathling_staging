import time
import pandas as pd
import json
import os
import subprocess
import docker
from collections import defaultdict
from typing import List, Dict
from io import StringIO

from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import requests

from PathlingViewDefinitionRunner import run_view_definition, ViewDefinition, get_column_names

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})

FHIR_SERVER_BASE_URL = os.environ.get("FHIR_SERVER_BASE_URL", "http://localhost:8082/fhir")
PATHLING_CONTAINER_NAME = "pathling-server-1"
PATHLING_BEARER_TOKEN = "f4a41f0d-fb75-45b5-88a7-461d4ad95f11"
PATHLING_BASE_URL = os.environ.get("PATHLING_BASE_URL", "http://localhost:8093/fhir")
FLARE_BASE_URL = os.environ.get("FLARE_BASE_URL", "http://localhost:8083")
STAGING_DIR = "/usr/share/staging"

SUPPORTED_RESOURCE_TYPES = ["Patient", "Observation", "Condition", "Consent", "Procedure", "MedicationAdministration",
                            "MedicationStatement", "Specimen"]


client = docker.from_env()


def start_pathling_service(compose_file_path):
    # Start services as defined in the docker-compose.yml file
    subprocess.run(["docker-compose", "-f", compose_file_path, "up", "-d"], check=True)
    print("Pathling service starting...")


    # Wait for the service to become healthy
    for _ in range(30):  
        service = client.containers.get(PATHLING_CONTAINER_NAME)
        health_status = service.attrs['State']['Health']['Status']
        if health_status == 'healthy':
            print("Pathling service is healthy.")
            return
        elif health_status == 'unhealthy':
            print("Pathling service has become unhealthy.")
            break
        time.sleep(10)  # Wait 10 seconds before the next retry as defined in the interval

    raise RuntimeError("Pathling service did not become healthy within the expected time.")

def stop_and_remove_pathling_service(compose_file_path):
    subprocess.run(["docker-compose", "-f", compose_file_path, "down", "-v"], check=True)
    print("Pathling service stopped and removed, including volumes.")


status_store = {
    'current_status': 'Idle'
}

def update_status(new_status):
    status_store['current_status'] = new_status

@app.route('/status')
def get_status():
    return jsonify(status_store)

@app.route("/run_ccdl", methods=["POST"])
def run_ccdl():
    ccdl = json.loads(request.get_data())
    structured_query = ccdl.get("sq")
    view_definitions = ccdl.get("viewDefinitions")

    print("Starting Pathling service...")
    update_status('Starting Pathling service...')

    try:
        start_pathling_service("pathling/docker-compose.yml")

        print("Getting patient ids...")
        update_status('Getting patient ids...')

        patient_ids = run_cohort_query(structured_query)

        print("Staging cohort data...")
        update_status('Staging cohort data...')

        response, status_code = stage_cohort_data(patient_ids)

        if status_code != 200:
            return response, status_code
        
        print("Running extraction...")
        update_status('Running extraction...')
        
        result = run_extraction(view_definitions)

        print("Done!")
        update_status('Done!')
    finally:
        print("Stopping Pathling service...")
        update_status('Stopping Pathling service...')
        stop_and_remove_pathling_service("pathling/docker-compose.yml")
        subprocess.run(["rm", "-f", "pathling/data/ndjson/*.ndjson"], check=True)
        update_status('Idle')

    return result

def run_cohort_query(structured_query):
    result = requests.post(f"{FLARE_BASE_URL}/execute-cohort", json=structured_query)
    result.raise_for_status()
    patient_ids = result.json()
    return patient_ids

def run_extraction(view_definitions):
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


def stage_cohort_data(patient_ids):
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


def process_and_import_fhir_bundle(bundle: dict):
    resources = [entry.get("resource") for entry in bundle["entry"] if entry.get("resource")]
    if not resources:
        return jsonify({"error": "No resources found"}), 404
    # Generate NDJSON files from the FHIR Bundle
    file_name_by_type = write_ndjson_by_resource_type(resources, "example")

    # Generate the parameters for the $import request
    parameters = create_parameters(file_name_by_type, STAGING_DIR)

    # Send the import request to the Pathling server
    return import_files_to_pathling(parameters, PATHLING_BASE_URL, PATHLING_BEARER_TOKEN)
    

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

if __name__ == "__main__":
    app.run(debug=True, port=8000)
