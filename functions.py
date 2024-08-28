import os 
import argparse
import sys
from kubernetes import client,config # Needed to interact with the cluster, pip install kubernetes
from kubernetes.client.rest import ApiException
import re

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
      config.load_kube_config(config_file=kubeconfig)
      contexts, active_context = config.list_kube_config_contexts()
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
  
# Function to retrieve bound PVCs in a cluster
def retrieve_pvcs_from_clusters(kube_config, target, source_context, target_context, no_output=False):
    config.load_kube_config(config_file=kube_config)
    # Retrieve the PVs from the source-context
    if (target=='source' or target=='both'):
      v1 = client.CoreV1Api(
        api_client=config.new_client_from_config(context=source_context)
      )
      source_volumes = v1.list_persistent_volume()
      # We'll ignore PVs that are not set to Bound
      bound_source_volumes = [source_volume for source_volume in source_volumes.items if source_volume.status.phase=='Bound']
      # Now let's write namespace:pvc-name into a file for every remaining PV if not no_output
      if not no_output:
        with open('source_pvcs.txt', 'w') as f:
          f.write(f'PVCs for source-context {source_context}:\n')
          for volume in bound_source_volumes:
            f.write(f'{volume.spec.claim_ref.namespace}:{volume.spec.claim_ref.name}\n') 
        
        # Inform the user the file has been written
        print(f'PVCs for source-context {source_context} written to source_pvcs.txt')
      # If no_output is True (so when it is called from another function instead of the user, just return the volumes)  
      else:
        # Let's format the volumes into a shape useful for the mapping-file check
        bound_source_volumes = [f'{volume.spec.claim_ref.namespace}:{volume.spec.claim_ref.name}' for volume in bound_source_volumes]
        return bound_source_volumes
    # Retrieve the PVs from the target-context
    if (target=='target' or target=='both'):
      v1 = client.CoreV1Api(
        api_client=config.new_client_from_config(context=target_context)
      )
      target_volumes = v1.list_persistent_volume()
      # We'll ignore PVs that are not set to Bound
      bound_target_volumes = [target_volume for target_volume in target_volumes.items if target_volume.status.phase=='Bound']
      # Now let's write namespace:pvc-name into a file for every remaining PV if not no_output
      if not no_output:
        with open('target_pvcs.txt', 'w') as f:
          f.write(f'PVCs for target-context {target_context}:\n')
          for volume in bound_target_volumes:
            f.write(f'{volume.spec.claim_ref.namespace}:{volume.spec.claim_ref.name}\n')
        
        # Inform the user the file has been written
        print(f'PVCs for target-context {target_context} written to target_pvcs.txt')
      # If no_output is True (so when called from another function instead of the user), just return the volumes  
      else:
        # Format volumes to be useful in the mapping file function
        bound_target_volumes = [f'{volume.spec.claim_ref.namespace}:{volume.spec.claim_ref.name}' for volume in bound_target_volumes]
        return bound_target_volumes

# Function to retrieve the active context from kube-config
def retrieve_source_context(kube_config):
  config.load_kube_config(config_file=kube_config)
  contexts, active_context = config.list_kube_config_contexts()
  return active_context['name']


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
      } for item in source_pvs_full.items if item.status.phase == 'Bound'
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
      } for item in target_pvs_full.items if item.status.phase == 'Bound'
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

# Function to match the retrieved PVs from source and target cluster in the situation where PVCs are named identically and in the same namespace (exact copy)
def match_pvs(source_pvs, target_pvs):
  matched_pvs = []
  # Debugging prints
  #print(source_pvs)
  #print(target_pvs)
  # Loop through the source_pvs list and match pvc_name and pvc_ns onto target_pvs
  for source_item in source_pvs:
      matching_target_item = next(
        (target_item for target_item in target_pvs if (target_item['pvc_name']==source_item['pvc_name'] and target_item['pvc_ns']==source_item['pvc_ns'])),
        None
      )
      # If we found a match, add the source_item and matching_target_item to the matched_pvs list
      if matching_target_item:
        matched_pvs.append({
          'source_pv' : source_item,
          'target_pv' : matching_target_item  
        })
  # Print statement for debugging purposes
  for item in matched_pvs:
    print(f"Source PV called {item['source_pv']['name']} matches with target PV called {item['target_pv']['name']}")
    print(f"Data dirs: {item['source_pv']['data_dir']} {item['target_pv']['data_dir']}")

# Function to check the validity of a supplied mapping file
def is_valid_mapping_file(mapping_file, kube_config, source_context, target_context):
  # Does every line in the file match the structure we expect?
  pattern = r'[a-z0-9]([-a-z0-9]*[a-z0-9])?'
  full_pattern = re.compile(rf'^{pattern}:{pattern},{pattern}:{pattern}$')

  with open(mapping_file, 'r') as file:
    for line_number, line in enumerate(file, start=1):
      line = line.strip()
      if not full_pattern.match(line):
        print(f"Error found in line {line_number} in mapping file. Lines should consist of namespace:pvc-name,namespace:pvc-name only. Namespace and PVCs should adhere to Kubernetes naming conventions.")
        return False
  
  # Now let's check if the PVCs passed in the mapping file actually exist in the cluster
  # Let's first split the mapping file into source and target PVCs.
  mapping_file_source_pvcs = []
  mapping_file_target_pvcs = []
  with open(mapping_file, 'r') as file:
    for line in file:
      line=line.strip()
      mapping_file_source_pvc, mapping_file_target_pvc = line.split(',')
      mapping_file_source_pvcs.append(mapping_file_source_pvc)
      mapping_file_target_pvcs.append(mapping_file_target_pvc)
  # Debugging print
  print(mapping_file_source_pvcs)
  # print(mapping_file_target_pvcs)
 
  # Now let's retrieve the PVCs using the retrieve_pvcs() function. We pass no_output as True this time to ensure it doesn't overwrite any existing files
  cluster_source_pvcs = retrieve_pvcs_from_clusters(kube_config, 'source', source_context, target_context, True)
  print( cluster_source_pvcs)









  return True

def test_function(mapping_file):
  listA = []
  listB = []
  with open(mapping_file, 'r') as file:
    for line in file:
      line=line.strip()
      source_pvc, target_pvc = line.split(',')
      listA.append(source_pvc)
      listB.append(target_pvc)
  print('Source PVCs:')
  print(listA)
  print('Target PVCs:')
  print(listB)
              