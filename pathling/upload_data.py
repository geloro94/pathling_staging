import os
import sys
import glob
import json
import dotenv
import requests
import subprocess

from json_to_ndjson import convert_fhir_bundles_to_ndjson

temp_dir = os.path.join('data', 'temp')
block_dir = os.path.join('data', 'block')
ndjson_dir = os.path.join('data', 'ndjson')


def chunks(lst, n):
    if n <= 0:
        yield lst
    else:
        for i in range(0, len(lst), n):
            yield lst[i:i + n]


def upload_archive(archive_path):
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    if not os.path.exists(ndjson_dir):
        os.makedirs(ndjson_dir)
    print(f"Processing file {archive_path}")
    # Decompress zipped file
    print("Decompressing file")
    p = subprocess.Popen(f'pigz -dc {archive_path} | tar -xf - -C {temp_dir}',
                         stdin=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    p_status = p.wait()

    bundle_files = glob.glob(pathname='data/temp/**/*.json', recursive=True)
    bundle_chunks = list(chunks(bundle_files, max_block_size))
    for chunk in bundle_chunks:
        if not os.path.exists(block_dir):
            os.makedirs(block_dir)

        # Move files in chunk
        for bundle_file in chunk:
            os.renames(bundle_file, os.path.join(block_dir, os.path.basename(bundle_file)))

        # Create NDJSON files and request
        print("Creating NDJSON bundles")
        convert_fhir_bundles_to_ndjson(block_dir, ndjson_dir)

        # Upload data
        request_file_path = os.path.join(ndjson_dir, 'request.json')
        request_url = f"http://localhost:8080/fhir/$import"
        print(f"Uploading data to {request_url}")
        response = requests.post(url=request_url, json=json.load(fp=open(request_file_path, mode='r')))
        if response.status_code < 300:
            print(f"Upload successful: {response.status_code}")
            # print("Removing temporary data")
            # subprocess.run(['rm', '-r', temp_dir])
            # subprocess.run(f"rm {os.path.join(ndjson_dir, '*.ndjson')}", shell=True)
            # subprocess.run(f"rm {os.path.join(ndjson_dir, '*.json')}", shell=True)
        else:
            print(f"Upload failed: {response.status_code}")
            print(response.text)
            exit(1)

        print("Removing temporary data")
        subprocess.run(f"rm -r {os.path.join(block_dir)}", shell=True)
        subprocess.run(f"rm {os.path.join(ndjson_dir, '*.ndjson')}", shell=True)
        subprocess.run(f"rm {os.path.join(ndjson_dir, '*.json')}", shell=True)

    # Create NDJSON files and request
    # print("Creating NDJSON bundles")
    # decompressed_data_path = os.path.join(temp_dir, 'output', 'fhir')
    # convert_fhir_bundles_to_ndjson(decompressed_data_path, ndjson_dir)

    # Upload data
    # request_file_path = os.path.join(ndjson_dir, 'request.json')
    # request_url = f"http://localhost:8080/fhir/$import"
    # print(f"Uploading data to {request_url}")
    # response = requests.post(url=request_url, json=json.load(fp=open(request_file_path, mode='r')))
    # if response.status_code < 300:
    #    print(f"Upload successful: {response.status_code}")
    #    print("Removing temporary data")
    #    subprocess.run(['rm', '-r', temp_dir])
    #    subprocess.run(f"rm {os.path.join(ndjson_dir, '*.ndjson')}", shell=True)
    #    subprocess.run(f"rm {os.path.join(ndjson_dir, '*.json')}", shell=True)
    # else:
    #    print(f"Upload failed: {response.status_code}")
    #    print(response.text)
    #    exit(1)

    print("Removing temporary data")
    subprocess.run(['rm', '-r', temp_dir])
    # subprocess.run(f"rm {os.path.join(ndjson_dir, '*.ndjson')}", shell=True)
    # subprocess.run(f"rm {os.path.join(ndjson_dir, '*.json')}", shell=True)


if __name__ == "__main__":
    assert len(sys.argv) >= 2, "Provide a path to the compressed data you want to upload"
    compressed_data_path = sys.argv[1]

    max_block_size = int(sys.argv[2]) if len(sys.argv) >= 3 else -1

    dotenv.load_dotenv('.env')
    pathling_port = os.environ.get('PATHLING_PORT')
    assert pathling_port is not None, "Set PATHLING_PORT environment variable in .env.default.default.default file"

    if os.path.isdir(compressed_data_path):
        print(f"Reading directory @ {compressed_data_path}")
        for file_path in glob.glob(pathname=os.path.join(compressed_data_path, '**/*.*'), recursive=True):
            upload_archive(os.path.join(compressed_data_path, file_path))
    elif os.path.isfile(compressed_data_path):
        print(f"Reading archive @ {compressed_data_path}")
        upload_archive(compressed_data_path)

    print("Done")
