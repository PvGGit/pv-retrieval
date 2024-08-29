import argparse
import sys
from functions import *

def main(args):
  kube_config = args.kube_config
  source_context = args.source_context
  target_context = args.target_context
  retrieve_pvcs = args.retrieve_pvcs
  mapping_file = args.mapping_file
 
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
  # If no source-context was passed, let's retrieve it from kube_config
  if not source_context:
    source_context = retrieve_source_context(kube_config)
    if source_context:
      print(f'Source context retrieved from kube-config: {source_context}')
    else:
      print(f'No active context was found in kube-config: {kube_config}')
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
  
  # If retrieve-pvcs was passed, call the retrieve_pvcs function
  if retrieve_pvcs:
    if ( (retrieve_pvcs=='both' or retrieve_pvcs=='target') and (not target_context)):
      print("Can't use options 'both' or 'target' without --target-context set")
    else:
      retrieve_pvcs_from_clusters(kube_config, retrieve_pvcs, source_context, target_context)

  # If no mapping file was provided and we have a target-context, we match PVs based on identical PVCs (name and ns) with source-cluster leading
  if ((not mapping_file) and (target_context)):
    retrieve_pvs(kube_config, source_context, target_context)

  # If a mapping file was passed, let's process it
  if mapping_file:
    if target_context:
      # Is it readable?
      if is_file_readable(mapping_file):
        print(f'Mapping file found to be readable at {mapping_file}')
      else:
        print(f'Mapping was not found or not readable at {mapping_file}')
        sys.exit(1)
      # If the file is found and readable, does it match the required structure?
      if is_valid_mapping_file(mapping_file, kube_config, source_context, target_context):
        print('Mapping file is valid')
        # Now that we're sure our mapping file is valid, let's get the datadirs/volumeHandles matched and returned
        retrieve_dirs_from_mapping_file(mapping_file, kube_config, source_context, target_context)
      else:
        print('Mapping file does not match required structure or contains incorrect PVCs')
    else:
      print('Please provide --target-context to be used with the mapping file')
    


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
  parser.add_argument('--retrieve-pvcs',
                      choices = ['source', 'target', 'both'],
                      help='Retrieve bound PVCs in cluster. Creates pvc_listing_<source/target>.txt file in the present working directory')
  parser.add_argument('--mapping-file',
                      type=str,
                      help='Path to mapping file')
  
  args = parser.parse_args()

  

  main(args)
