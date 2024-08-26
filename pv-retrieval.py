import os 
import argparse
import sys
from kubernetes import client,config # Needed to interact with the cluster, pip install kubernetes
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
def check_context_connectivity(kube_config, context):
  try:
      # Get config loaded up and extract contexts and active_context from the config
      config.load_kube_config(config_file=kube_config)
      contexts, active_context = config.list_kube_config_contexts()
      
      # Are there contexts in the kubeconfig file?
      if not contexts:
        print('No contexts found in kubeconfig')
        return False
      
      # Use the active context if no parameter was passed for context and there is an active_context
      if (not context) and active_context:
        context = active_context['name']

      # Is the context (either passed or from active_context) present in the kubeconfig file?
      contexts = [item['name'] for item in contexts]
      if context not in contexts:
        print(f'The context {context} was not found in the kubeconfig file')
        return False
      
      # If the passed context is present, try to list namespaces
      else:
        v1 = client.CoreV1Api(
          api_client=config.new_client_from_config(context=context)
        )
        namespaces = v1.list_namespace()
        if namespaces:
          print('Cluster connectivity is alright!')
          return True
        else:
          print(f'No namespaces found in context {context}')
          return False
        
  # Handle exceptions      
  except ApiException as e:
    print(f'Error when connecting to cluster: {e}')
    return False
  except Exception as e:
    print(f'An unexpected error occurred: {e}')
    return False

# Function to retrieve existing PersistentVolumes from a cluster and their datadirectory (if present)
def retrieve_pvs(kube_config):
  config.load_kube_config(config_file=kube_config)
  v1 = client.CoreV1Api()
  # Retrieve PersistentVolumes and list them if present
  persistent_volumes = v1.list_persistent_volume()
  if persistent_volumes:
    print('The following PersistentVolumes were found:')
    for item in persistent_volumes.items:
      print(item.metadata.name)
    return True
  else:
    print('No PersistentVolumes were found in the cluster.')
    return False
  

def main(args):
  # First check if a kube_config was passed and if so, if it is a valid file
  kube_config = args.kube_config
  source_context = args.source_context
  target_context = args.target_context
  # If kube-config was passed, let's see if we can access it
  if kube_config:
    if is_file_readable(kube_config):
      print('Kube config passed and found to be a readable file')
    else:
      print('Please provide a valid path when using --kube-config')
      sys.exit(1)
  # If kube-config was not passed, then let's see if we can retrieve a valid path from $KUBECONFIG
  else:
    kube_config=retrieve_kubeconfig_env()
    if kube_config:
      print(f'Kubeconfig retrieved from KUBECONFIG environment variable: {kube_config}')
    else:
      print('No kube-config was passed, and no valid config file found in KUBECONFIG environment variable.')
      sys.exit(1)
  # Now let's find out if kube-config or KUBECONFIG is valid to connect to the source cluster
  if check_context_connectivity(kube_config, source_context):
    print('Source cluster connectivity verified.')
  else:
    print(f'Source cluster connectivity failed. Please check the context definition in {kube_config}')
    sys.exit(1)
  # If target-context was passed, let's check if we can connect to the context specified
  if target_context:
    if check_context_connectivity(kube_config, target_context):
      print('Target cluster connectivity verified')
    else:
      print(f'Target cluster connectivity failed. Please check the context definition in {kube_config}')
      sys.exit(1)
  # If --retrieve-pvs was used, invoke the function to retrieve the PVs.
  if args.retrieve_pvs:
    retrieve_pvs(kube_config)



if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--kube-config', 
                      type=str,
                      help='Path to the kubeconfig for the source cluster')
  parser.add_argument('--retrieve-pvs',
                      action = 'store_true',
                      help = 'Retrieve PVs from the targeted cluster.'  )
  parser.add_argument('--source-context',
                      type=str,
                      help='Define the source context to be used. Will use active context if present')
  parser.add_argument('--target-context',
                      type=str,
                      help='Define the source context to be used. Does not default to active context')
  args = parser.parse_args()

  

  main(args)
