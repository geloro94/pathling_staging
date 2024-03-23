import os
import glob
import json
import copy
import random
import argparse
import requests
import datetime
import subprocess

pathling_project_name = 'feasibility-query-performance-pathling'
query_path = 'query'
query_file_pattern = '*.json'
result_path = 'result'
aggregate_query_template = json.load(open(os.path.join('data', 'template', 'aggregate_query.json'), mode='r'))
headers = {
    'Content-Type': "application/fhir+json"
}


def load_queries(path, file_pattern):
    print("Loading queries")
    query_sets = {}

    # Detect test directories
    for dir_name in os.listdir(path):
        if os.path.isdir(os.path.join(path, dir_name)):
            # Get query files in directory
            queries = {}
            for query_file in glob.glob(os.path.join(path, dir_name, file_pattern)):
                key = os.path.splitext(os.path.basename(query_file))[0]
                queries[key] = json.load(open(query_file, encoding='utf8'))
            query_sets[dir_name] = queries

    print(f"Loaded query sets: {', '.join(query_sets.keys())}")
    return query_sets


def generate_result_sets(query_sets):
    result_sets = {}
    for test_name, queries in query_sets.items():
        test_set = {}
        result_sets[test_name] = test_set
        for query_name, query in queries.items():
            test_set[query_name] = []
    return result_sets


def generate_test_run_order(query_sets, num_pre_run_queries=None):
    # Flatten dictionary
    total_query_set = []
    for test_name, queries in query_sets.items():
        for query_name, query in queries.items():
            total_query_set.append((test_name, query_name, query))

    # Determine number of queries to run prior to recording elapsed time if not provided
    if num_pre_run_queries is None:
        num_pre_run_queries = max(0, len(total_query_set))
    pre_run_queries = random.sample(total_query_set, num_pre_run_queries)

    # Randomize order
    random.shuffle(total_query_set)

    return pre_run_queries, total_query_set


def calculate_avg(times):
    total = datetime.timedelta()
    cnt = 0
    for time in times:
        if time is not None:
            total += time
            cnt += 1
    return total / cnt if cnt != 0 else None


def generate_aggregate_request_body(query):
    body = copy.deepcopy(aggregate_query_template)
    parameters = body['parameter']
    for filter_stmt in query.get('filter', []):
        parameters.append({
            'name': "filter",
            'valueString': filter_stmt
        })
    return body


def restart_containers(project):
    print(f"Restarting containers for project '{project}'")
    subprocess.run(['docker', 'compose', '--project-name', project, 'down'])
    subprocess.run(['docker', 'compose', '--project-name', project, 'up', '--wait'])


def run_test(query_sets, url, project_name, rounds=None, num_pre_run_queries=None, timeout=1800):
    if rounds is None:
        rounds = 1
    if num_pre_run_queries is None:
        num_pre_run_queries = 0

    print(f"Running tests: rounds: {rounds}, number of pre-run queries: {num_pre_run_queries}")
    result_sets = generate_result_sets(query_sets)

    for i in range(1, rounds + 1):
        print(f"Round {i}")
        pre_run_query_set, query_set = generate_test_run_order(query_sets, num_pre_run_queries)

        restart_containers(project_name)

        # Run pre-run queries
        print("Running pre-run queries")
        for test_name, query_name, query in pre_run_query_set:
            print(f"Query [{test_name}]{query_name}")
            # response = requests.post(url=f"{url}/Patient/_search", data=query, headers=headers)
            try:
                response = requests.post(url=f"{url}/Patient/$aggregate",
                                         json=generate_aggregate_request_body(query),
                                         headers=headers,
                                         timeout=timeout)
                if response.status_code != 200:
                    print(f"Error while running pre-run query '{test_name}#{query_name}'")
                    print(f"Status code: {response.status_code}. Reason:\n{response.text}")
            except Exception as exc:
                print(f"Error while running pre-run query '{test_name}#{query_name}'")
                print(f"Reason: {repr(exc)}")

        # Run queries
        print("Running queries")
        for test_name, query_name, query in query_set:
            print(f"Query [{test_name}]{query_name}")
            # response = requests.post(url=f"{url}/Patient/_search", data=query, headers=headers)
            try:
                response = requests.post(url=f"{url}/Patient/$aggregate",
                                         json=generate_aggregate_request_body(query),
                                         headers=headers,
                                         timeout=timeout)
                if response.status_code == 200:
                    time_elapsed = response.elapsed
                    result_sets[test_name][query_name].append({
                        'time': time_elapsed,
                        'result': response.text
                    })
                    print(f"Success: Time elapsed: {time_elapsed}")
                    print(response.text)
                else:
                    result_sets[test_name][query_name].append({
                        'time': None,
                        'result': response.text
                    })
                    print(f"Failure: {response.status_code}. Reason:\n{response.text}")
            except Exception as exc:
                result_sets[test_name][query_name].append({
                    'time': None,
                    'result': repr(exc)
                })
                print(f"Failure: {str(exc)}")


    # Generate report
    print("Processing results")
    report = {}
    for test_name, queries in result_sets.items():
        test_results = {}
        report[test_name] = test_results
        for query_name, query_results in queries.items():
            print(query_results)
            result_entry = {
                'avg': str(calculate_avg([result['time'] for result in query_results])),
                'times': [str(result['time']) for result in query_results],
                'results': [str(result['result']) for result in query_results]
            }
            test_results[query_name] = result_entry

    print("Done")
    return report


#    for test_name, queries in query_sets.items():
#        result_set = {}
#        print(f"\tRunning test set '{test_name}'")
#        for query_name, query in queries.items():
#            sum_time = datetime.timedelta()
#
#            for i in range(1, rounds + 1):
#                print(f"\t\t[Round {i}] Posting query {query_name}")
#                response = requests.post(url=f"{url}/Patient/_search", data=query, headers=headers)
#
#                if response.status_code == 200:
#                    time_elapsed = response.elapsed
#                    sum_time += time_elapsed
#                    print(f"\t\t\tSuccess: Time elapsed: {time_elapsed}")
#                else:
#                    print(f"\t\t\tFailure: {response.status_code}. Reason: {response.text}")
#
#            result_set[query_name] = {'avg': str(sum_time/rounds)}
#
#        result_sets[test_name] = result_set
#
#    print("Done")
#    return result_sets


def configure_argparse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--rounds', type=int, default=10, help='Number of rounds in which each query is run')
    parser.add_argument('-p', '--num-pre-queries', type=int, default=0,
                        help='number of queries to run before running queries each round')
    parser.add_argument('-u', '--url', required=True, help='Pathling URL to send requests to')
    parser.add_argument('-f', '--file', default=os.path.join(result_path, 'result_fhirbase_' +
                                                             datetime.datetime.today().strftime('%Y-%m-%d#%H:%M:%S') +
                                                             '.json'), help='Output file for report')
    parser.add_argument('-t', '--timeout', type=int, default=1800, help='Response time limit for requests')
    return parser


if __name__ == "__main__":
    arg_parser = configure_argparse()
    args = arg_parser.parse_args()

    base_url = args.url
    num_rounds = args.rounds
    num_pre_run_queries = args.num_pre_queries
    request_timeout = args.timeout

    pathling_query_sets = load_queries(query_path, query_file_pattern)
    pathling_test_result = run_test(pathling_query_sets, base_url, pathling_project_name, num_rounds,
                                    num_pre_run_queries, request_timeout)

    json.dump(pathling_test_result, fp=open(args.file, mode='w+'), indent=2)
