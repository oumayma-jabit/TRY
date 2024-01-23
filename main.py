import oci
import os
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configuration
oci_config = oci.config.from_file()
object_storage_client = oci.object_storage.ObjectStorageClient(oci_config)
config = oci.config.from_file(oci.config.DEFAULT_LOCATION, oci.config.DEFAULT_PROFILE)
reporting_bucket = config['tenancy']
reporting_namespace = 'bling'

destination_path = 'REPORTS'  # Destination directory for decompressed files

# Make a directory to store decompressed files
if not os.path.exists(destination_path):
    os.makedirs(destination_path)


# Initialize a counter for the number of files downloaded
files_downloaded = 0

# Marker to track the last object name retrieved
marker = None


def download_and_decompress(object_name):
    try:
        object_details = object_storage_client.get_object(
            namespace_name=reporting_namespace, bucket_name=reporting_bucket, object_name=object_name
        )
        # Check if the file is already downloaded
        if any(filename == object_name.rsplit('/', 1)[-1][:-3] for filename in os.listdir(destination_path)):
            return None

        # Download the object
        compressed_data = object_details.data.raw.data

        # Decompress the data
        decompressed_data = gzip.decompress(compressed_data)

        # Extract the filename
        filename = object_name.rsplit('/', 1)[-1][:-3]

        # Save the decompressed data to the destination path
        with open(os.path.join(destination_path, filename), 'wb') as f:
            f.write(decompressed_data)
            return object_name
    except Exception as e:
        print(f"Error downloading/decompressing {object_name}: {e}")
        return None


# Number of concurrent threads
num_threads = 15  # Adjust based on your system capabilities

start = time.time()
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    while True:
        report_bucket_objects = object_storage_client.list_objects(
            namespace_name=reporting_namespace,
            bucket_name=reporting_bucket,
            prefix="reports/cost-csv",
            start=marker
        )

        # Use executor.map to parallelize the download and decompression
        futures = [executor.submit(download_and_decompress, o.name) for o in report_bucket_objects.data.objects]

        # Wait for all futures to complete
        for future in as_completed(futures):
            result = future.result()
            if result:
                files_downloaded += 1
                print('----> File ' + result + ' Decompressed and Saved', files_downloaded)

        # Update the marker for the next page
        marker = report_bucket_objects.data.next_start_with

        if not marker:
            break
print(time.time() - start)
print(f"Total files downloaded: {files_downloaded}")

