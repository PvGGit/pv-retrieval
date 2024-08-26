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

# Function to list existing PersistentVolumes from a cluster
def list_pvs(kube_config, context):
  config.load_kube_config(config_file=kube_config)
  v1 = client.CoreV1Api(
    api_client=config.new_client_from_config(context=context)
  )
  # List PersistentVolumes and return if found
  persistent_volumes = v1.list_persistent_volume()
  if persistent_volumes:
    return persistent_volumes
  else:
    return False

# Function to retrieve PVs from both clusters and match them together
def retrieve_pvs(kube_config, source_context, target_context):
  source_pvs_full = list_pvs(kube_config, source_context)
  # If PVs were returned for source-context, we continue on to the target context
  if source_pvs_full:
    # Let's extract just the values we need for our purposes
    source_pvs = [
      {'name': item.metadata.name if item.metadata.name else 'not defined',
      'pvc_name': item.spec.claim_ref.name if item.spec.claim_ref.name else 'not defined',
      'pvc_ns': item.spec.claim_ref.namespace if item.spec.claim_ref.namespace else 'not defined',
      'data_dir': item.spec.nfs.path if item.spec.nfs.path else 'not defined'
      } for item in source_pvs_full.items
    ]
    # Print statement below for debugging purposes
    #print(f'source_pvs is populated with: {source_pvs}')
    target_pvs_full = list_pvs(kube_config, target_context)
    # If PVs were returned for target-context, we call the matching function
    if target_pvs_full:
      target_pvs = [
      {'name': item.metadata.name if item.metadata.name else 'not defined',
      'pvc_name': item.spec.claim_ref.name if item.spec.claim_ref.name else 'not defined',
      'pvc_ns': item.spec.claim_ref.namespace if item.spec.claim_ref.namespace else 'not defined',
      'data_dir': item.spec.nfs.path if item.spec.nfs.path else 'not defined'
      } for item in target_pvs_full.items
      ]
      # Print statement below for debugging purposes
      #print(f'target_pvs is populated with {target_pvs}')
      
      # Now that we have both dicts populated, it's time to match them
      match_pvs(source_pvs, target_pvs)
    else:
      print(f'No PersistentVolumes were found in context {target_context}')
      sys.exit(1)
  else:
    print(f'No PersistentVolumes were found in context {source_context}')
    sys.exit(1)

# Function to match the retrieved PVs from source and target cluster
def match_pvs(source_pvs, target_pvs):
  matched_pvs = []
  # Debugging prints
  print(source_pvs)
  print(target_pvs)
  # Loop through the source_pvs list and match pvc_name and pvc_ns onto target_pvs
  """
  for source_item in source_pvs:
      matching_target_item = [target_item for target_item in target_pvs if (target_item.pvc_name==source_item.pvc_name and target_item.pvc_ns==source_item.pvc_ns)]
      # If we found a match, add the source_item and matching_target_item to the matched_pvs list
      if matching_target_item:
        matched_pvs.append({
          'source_pv' : source_item,
          'target_pv' : matching_target_item  
        })
  """
  # Print statement for debugging purposes
  print(matched_pvs)




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
      if is_file_readable(kube_config):
        print(f'Kubeconfig retrieved from KUBECONFIG environment variable: {kube_config}')
      else:
        print(f'Kubeconfig retrieved from KUBECONFIG environment variable, but file is not readable: {kube_config}')
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
  # Now that we have both source and target contexts validated, we call upon retrieve_pvs to do its magic.
  retrieve_pvs(kube_config, source_context, target_context)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--kube-config', 
                      type=str,
                      help='Path to the kubeconfig file. Defaults to the KUBECONFIG environment variable if not specified')
  parser.add_argument('--source-context',
                      type=str,
                      help='Define the source context to be used. Will use active context if present')
  parser.add_argument('--target-context',
                      type=str,
                      help='Define the source context to be used. Does not default to active context')
  args = parser.parse_args()

  

  main(args)
