import argparse
import sys
import functions

def main(args):
  # First check if a kube_config was passed and if so, if it is a valid file
  kube_config = args.kube_config
  source_context = args.source_context
  target_context = args.target_context
  retrieve_pvcs = args.retrieve_pvcs
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
  # Now that we have both source and target contexts validated, we call upon retrieve_pvs to do its magic.
  # Commented out for now, development purposes
  # retrieve_pvs(kube_config, source_context, target_context)
  # If retrieve-pvcs was passed, call the retrieve_pvcs function
  if retrieve_pvcs:
    retrieve_pvcs_from_clusters(kube_config, retrieve_pvcs, source_context, target_context)


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
                      help='Retrieve bound PVCs in cluster. Creates pvc_listing_<choice>.txt file in the present working directory')
  args = parser.parse_args()

  

  main(args)
