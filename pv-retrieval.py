import os 
import argparse
import sys
from kubernetes import client,config # Needed to interact with the cluster
from kubernetes.client.rest import ApiException

# Function to check if filepaths exist and are readable.

def is_file_readable(file_path):
  # Is it an existing file?
  if os.path.isfile(file_path):
    # Can we read the file?
    if os.access(file_path, os.R_OK):
      return True
    else:
      print(f'File exists but is not readable: {file_path}')
      return False
  else:
    print(f'File does not exist or is not a file: {file_path}')
    return False

# Function to retrieve KUBECONFIG from the environment variables if present

def retrieve_kubeconfig_env():
  if 'KUBECONFIG' in os.environ:
    kubeconfig=os.environ.get('KUBECONFIG')
    print(f'KUBECONFIG was found in environment variables: {kubeconfig}')
    # Is the file readable? If so, return it to its caller
    if is_file_readable(kubeconfig):
      return kubeconfig
    else:
      return False
  else:
    print('No KUBECONFIG found in environment variables')
    return False

# Function to check cluster connectivity. For now, we'll assume that being able to retrieve all namespaces is sufficient.
def test_cluster_connectivity(source_config):
  try:
      config.load_kube_config(config_file=source_config)
      v1 = client.CoreV1Api()
      namespaces = v1.list_namespace()
      print('Cluster connectivity is alright!')
      return True
  except ApiException as e:
    print(f'Error when connecting to cluster: {e}')
    return False
  except Exception as e:
    print(f'An unexpected error occurred: {e}')
    return False

# Function to retrieve existing PersistentVolumes from a namespace and their datadirectory (if present)

def retrieve_pvs(kube_config):
  config.load_kube_config(config_file=kube_config)
  v1 = client.CoreV1Api()
  # Retrieve PersistentVolumes and list them if present
  persistent_volumes = v1.list_persistent_volume()
  if persistent_volumes:
    print(f'The following PersistentVolumes were found: {persistent_volumes.items[0].metadata.name}')
    return True
  else:
    print('No PersistentVolumes were found in the cluster.')
    return True
  

# Function to check we have a kubeconf at our disposal. If the --source-config parameter was passed, we'll use that.
# If the parameter wasn't passed, we use the $KUBECONFIG environment variable if it exists
# If neither are present, we stop and tell the user to fix it.

def main(args):
  # First check if a source_config was passed and if so, if it is a valid file
  source_config = args.source_config
  if source_config:
    if is_file_readable(source_config):
      print('Source config passed and found to be a readable file')
    else:
      print('Please provide a valid path when using --source-config')
      sys.exit(1)
  # If not, then let's see if we can retrieve a valid path from $KUBECONFIG
  else:
    source_config=retrieve_kubeconfig_env()
    if source_config:
      print(f'Kubeconfig retrieved from KUBECONFIG environment variable: {source_config}')
    else:
      print('No source kubeconfig was passed, and no valid config file found in KUBECONFIG environment variable.')
      sys.exit(1)
  # Now let's find out if the source config or KUBECONFIG is valid to connect to the cluster
  if test_cluster_connectivity(source_config):
    print('Cluster connectivity verified.')
  else:
    print('Cluster connectivity failed. Please provided a valid kubeconf file, either through --source-config or by setting the KUBECONFIG variable')
    sys.exit(1)
  # If --retrieve-pvs was used, invoke the function to retrieve the PVs.
  if args.retrieve_pvs:
    retrieve_pvs(source_config)

  

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--source-config', 
                      type=str,
                      help='Path to the kubeconfig for the source cluster')
  parser.add_argument('--retrieve-pvs',
                      action = 'store_true',
                      help = 'Retrieve PVs from the targeted cluster.'  )
  args = parser.parse_args()

  main(args)
