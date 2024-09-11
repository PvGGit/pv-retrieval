import os
from kubernetes import client, config  # type: ignore
import re
from typing import Union, List, Optional

# Imports for type hinting purposes
from kubernetes.client.models import V1PersistentVolumeList, V1PersistentVolume # type: ignore

# Function to retrieve KUBECONFIG from the environment variables if present
def retrieve_kubeconfig_env() -> str:
    """
    Retrieves the 'KUBECONFIG' environment variable.

    This function attempts to retrieve the 'KUBECONFIG' environment variable, which specifies the path
    to the kubeconfig file used by Kubernetes. If the environment variable is not set, a `RuntimeError`
    is raised.

    Returns:
        str: The value of the 'KUBECONFIG' environment variable.

    Raises:
        RuntimeError: If 'KUBECONFIG' is not found in the environment variables, an error is raised with a message
        instructing the user to specify a valid kubeconfig file.
    """
    try:
        return os.environ['KUBECONFIG']
    except KeyError:
        raise RuntimeError(
            'No KUBECONFIG found in environment variables, please specify a valid kube-config file'
        )


# Function to list existing PersistentVolumes from a cluster
def list_pvs(kube_config: str, context: str) -> V1PersistentVolumeList:
    """
    Lists Persistent Volumes (PVs) in a specified Kubernetes context.

    This function loads the specified Kubernetes configuration file and uses the provided context
    to create a connection to the Kubernetes cluster. It then retrieves and returns the list of
    Persistent Volumes (PVs) in the cluster.

    Args:
        kube_config (str): Path to the kubeconfig file that contains the cluster configuration.
        context (str): The context within the kubeconfig file to connect to.

    Returns:
        V1PersistentVolumeList: A list of Persistent Volumes (PVs) in the Kubernetes cluster.

    """
    config.load_kube_config(config_file=kube_config)
    v1 = client.CoreV1Api(api_client=config.new_client_from_config(context=context))
    return v1.list_persistent_volume()

def get_bound_pvcs(kube_config: str, context: str) -> list:
    """
    Retrieves a list of bound Persistent Volume Claims (PVCs) from a specified Kubernetes context.

    This function loads the specified Kubernetes configuration file and uses the provided context
    to connect to the Kubernetes cluster. It fetches the list of Persistent Volumes (PVs) and
    returns the PVCs that are in the 'Bound' state, formatted as 'namespace:pvc_name'.

    Args:
        kube_config (str): Path to the kubeconfig file that contains the cluster configuration.
        context (str): The context within the kubeconfig file to connect to.

    Returns:
        list: A list of strings representing bound PVCs, formatted as 'namespace:pvc_name'.
    """
    config.load_kube_config(config_file=kube_config)
    v1 = client.CoreV1Api(
        api_client=config.new_client_from_config(context=context)
    )
    volumes = v1.list_persistent_volume()
    bound_volumes = [
        f'{volume.spec.claim_ref.namespace}:{volume.spec.claim_ref.name}'
        for volume in volumes.items
        if volume.status.phase == 'Bound'
    ]
    return bound_volumes

# Function to retrieve bound PVCs in a cluster
def retrieve_pvcs_from_clusters(
    kube_config: str,
    target: str,
    source_context: str,
    target_context: str,
) -> Union[None, List[str]]:
    """
    Retrieves Persistent Volume Claims (PVCs) from specified Kubernetes clusters and writes them to files.

    This function connects to two Kubernetes clusters specified by the `source_context` and `target_context`.
    Based on the `target` argument, it retrieves the bound PVCs from either the source cluster, target cluster,
    or both. The PVCs are written to text files, one for each context, with the format 'namespace:pvc_name'.
    It also prints a message indicating the file has been written.

    Args:
        kube_config (str): Path to the kubeconfig file used to connect to the clusters.
        target (str): Specifies the target to retrieve PVCs from; accepts 'source', 'target', or 'both'.
        source_context (str): The Kubernetes context for the source cluster.
        target_context (str): The Kubernetes context for the target cluster.

    Returns:
        None: This function returns `None` after writing the PVCs to files.
    """
    config.load_kube_config(config_file=kube_config)
    # Retrieve the PVs from the source-context
    if target == 'source' or target == 'both':
        bound_source_volumes = get_bound_pvcs(kube_config, source_context)
        # Now let's write namespace:pvc-name into a file for every remaining PV if not no_output
        with open('source_pvcs.txt', 'w') as f:
            f.write(f'PVCs for source-context {source_context}:\n')
            for item in bound_source_volumes:
                f.write(f'{item}\n')
        # Inform the user the file has been written
        print(
            f'PVCs for source-context {source_context} written to source_pvcs.txt'
        )
    # Retrieve the PVs from the target-context
    if target == 'target' or target == 'both':
        bound_target_volumes = get_bound_pvcs(kube_config, target_context)
        # Now let's write namespace:pvc-name into a file for every remaining PV if not no_output
        with open('target_pvcs.txt', 'w') as f:
            f.write(f'PVCs for target-context {target_context}:\n')
            for item in bound_target_volumes:
                f.write(f'{item}\n')
        # Inform the user the file has been written
        print(
            f'PVCs for target-context {target_context} written to target_pvcs.txt'
        )
    return None


# Function to retrieve the active context from kube-config
def retrieve_source_context(kube_config: str) -> str:
    """
    Retrieves the active context name from a specified Kubernetes configuration file.

    This function loads the specified kubeconfig file and retrieves the active context name
    from the Kubernetes configuration. The active context determines which cluster and namespace
    the kubectl commands will interact with.

    Args:
        kube_config (str): Path to the kubeconfig file used to load the Kubernetes configuration.

    Returns:
        str: The name of the active context from the kubeconfig file.
    """
    config.load_kube_config(config_file=kube_config)
    _, active_context = config.list_kube_config_contexts()
    return active_context['name']


# Function to retrieve PVs from both clusters and match them together
def retrieve_pvs(kube_config: str, source_context: str, target_context: str) -> None:
    """
    Retrieves and processes Persistent Volumes (PVs) from source and target Kubernetes contexts,
    and matches them based on relevant criteria.

    This function first retrieves the Persistent Volumes (PVs) from the source Kubernetes context and extracts
    only those that are in the 'Bound' state. For each PV, the function extracts the name, associated PVC name,
    namespace, and data directory.

    The same process is then applied to the target Kubernetes context, provided that PVs were successfully retrieved
    from the source context. Once both source and target PVs have been processed, the function calls `match_pvs` to
    perform the matching between the two sets of PVs.

    If no PVs are found in either the source or target context, a `RuntimeError` is raised.

    Args:
        kube_config (str): Path to the kubeconfig file used to load the Kubernetes configuration.
        source_context (str): The Kubernetes context for the source cluster from which PVs are retrieved.
        target_context (str): The Kubernetes context for the target cluster from which PVs are retrieved and matched.

    Raises:
        RuntimeError: If no PVs are found in either the source or target context.

    Returns:
        None
    """
    source_pvs_full = list_pvs(kube_config, source_context)
    # If PVs were returned for source-context, we continue on to the target context
    if source_pvs_full:
        # Let's extract just the values we need for our purposes
        source_pvs = [
            {
                'name': item.metadata.name if item.metadata.name else 'not defined',
                'pvc_name': (
                    item.spec.claim_ref.name
                    if item.spec.claim_ref.name
                    else 'not defined'
                ),
                'pvc_ns': (
                    item.spec.claim_ref.namespace
                    if item.spec.claim_ref.namespace
                    else 'not defined'
                ),
                'data_dir': (
                    item.spec.nfs.path if item.spec.nfs else item.spec.csi.volume_handle
                ),
            }
            for item in source_pvs_full.items
            if item.status.phase == 'Bound'
        ]

        target_pvs_full = list_pvs(kube_config, target_context)
        # If PVs were returned for target-context, we call the matching function
        if target_pvs_full:
            target_pvs = [
                {
                    'name': item.metadata.name if item.metadata.name else 'not defined',
                    'pvc_name': (
                        item.spec.claim_ref.name
                        if item.spec.claim_ref.name
                        else 'not defined'
                    ),
                    'pvc_ns': (
                        item.spec.claim_ref.namespace
                        if item.spec.claim_ref.namespace
                        else 'not defined'
                    ),
                    'data_dir': (
                        item.spec.nfs.path
                        if item.spec.nfs
                        else item.spec.csi.volume_handle
                    ),
                }
                for item in target_pvs_full.items
                if item.status.phase == 'Bound'
            ]

            # Now that we have both dicts populated, it's time to match them
            match_pvs(source_pvs, target_pvs)
        else:
            raise RuntimeError(
                f'No PersistentVolumes were found in context {target_context}'
            )
    else:
        raise RuntimeError(
            f'No PersistentVolumes were found in context {source_context}'
        )


# Function to match the retrieved PVs from source and target cluster in the situation where PVCs are named identically and in the same namespace (exact copy)
def match_pvs(source_pvs: list, target_pvs: list) -> None:
    """
    Matches Persistent Volumes (PVs) from source and target clusters based on PVC name and namespace.

    This function compares two lists of PVs, one from the source cluster and one from the target cluster,
    and attempts to match them based on the PVC name (`pvc_name`) and namespace (`pvc_ns`). If a match is
    found, the matching source and target PVs are added to a list of matched PVs. The function then prints
    debugging information about the matched PVs. If no matches are found, a `RuntimeError` is raised.

    Args:
        source_pvs (list): A list of dictionaries representing PVs from the source cluster. Each dictionary contains
                           keys such as 'name', 'pvc_name', 'pvc_ns', and 'data_dir'.
        target_pvs (list): A list of dictionaries representing PVs from the target cluster, structured similarly
                           to `source_pvs`.

    Raises:
        RuntimeError: If no matching PVs are found between the source and target clusters, the function raises an
                      error prompting the user to supply a mapping file for manual resolution.

    Returns:
        None: The function does not return any value, but it prints information about matched PVs or raises an exception.
    """
    matched_pvs = []

    # Loop through the source_pvs list and match pvc_name and pvc_ns onto target_pvs
    for source_item in source_pvs:
        matching_target_item = next(
            (
                target_item
                for target_item in target_pvs
                if (
                    target_item['pvc_name'] == source_item['pvc_name']
                    and target_item['pvc_ns'] == source_item['pvc_ns']
                )
            ),
            None,
        )
        # If we found a match, add the source_item and matching_target_item to the matched_pvs list
        if matching_target_item:
            matched_pvs.append(
                {'source_pv': source_item, 'target_pv': matching_target_item}
            )
    if matched_pvs:
        # Print statement for debugging purposes
        for item in matched_pvs:
            print(
                f"Source PV called {item['source_pv']['name']} matches with target PV called {item['target_pv']['name']}"
            )
            print(
                f"Data dirs: {item['source_pv']['data_dir']} {item['target_pv']['data_dir']}"
            )
    else:
        raise RuntimeError(
            'PVCs in source and target cluster are not identical. Please supply a mapping file using the --mapping-file parameter instead.'
        )


# Simple function to select PVs based on properties
def select_pv_on_pvc(
    pv_list: list, ns: str, pvc_name: str
) -> Optional[V1PersistentVolume]:
    """
    Selects a Persistent Volume (PV) from a list based on the specified namespace and PVC name.

    This function iterates over a list of Persistent Volumes (PVs) and returns the PV that matches
    the provided namespace (`ns`) and Persistent Volume Claim (PVC) name (`pvc_name`). If no matching
    PV is found, the function returns `None`.

    Args:
        pv_list (list): A list of `V1PersistentVolume` objects to search through.
        ns (str): The namespace of the PVC to match.
        pvc_name (str): The name of the PVC to match.

    Returns:
        Optional[V1PersistentVolume]: The matching `V1PersistentVolume` object if found,
                                      otherwise `None`.
    """
    for pv in pv_list:
        if pv.spec.claim_ref.namespace == ns and pv.spec.claim_ref.name == pvc_name:
            return pv
    return None


def retrieve_dirs_from_mapping_file(
    mapping_file: str, kube_config: str, source_context: str, target_context: str
) -> None:
    """
    Validates and processes Persistent Volume Claims (PVCs) from a mapping file and retrieves the corresponding
    Persistent Volumes (PVs) from both source and target Kubernetes clusters.

    This function reads a mapping file that specifies PVCs in the format 'namespace:pvc-name,namespace:pvc-name',
    validates that the PVC names and namespaces conform to Kubernetes naming conventions, and ensures that the PVCs
    exist in both the source and target clusters. It retrieves the corresponding Persistent Volumes (PVs) from the
    clusters and matches them based on their PVCs, comparing details such as NFS paths or Ceph volume handles.

    Args:
        mapping_file (str): The path to the mapping file that contains PVCs from both the source and target clusters
                            in the format 'namespace:pvc-name,namespace:pvc-name'.
        kube_config (str): The path to the kubeconfig file used to authenticate and connect to the Kubernetes clusters.
        source_context (str): The Kubernetes context for the source cluster from which the PVCs and PVs are retrieved.
        target_context (str): The Kubernetes context for the target cluster from which the PVCs and PVs are retrieved.

    Raises:
        RuntimeError: If the entries in the mapping file are not properly formatted, if PVCs from the mapping file
                      do not exist in the specified clusters, or if no bound PVs are found in either the source or
                      target clusters. This error may also occur if a matching PV is not found for a given PVC.

    Returns:
        None: This function prints the matched PV details from the source and target clusters for debugging and
              comparison purposes, but does not return a value.
    """
    # Do entries in the mapping file conform to the expected pattern?
    pattern = r'[a-z0-9]([-a-z0-9]*[a-z0-9])?'
    full_pattern = re.compile(rf'^{pattern}:{pattern},{pattern}:{pattern}$')

    with open(mapping_file, 'r') as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not full_pattern.match(line):
                raise RuntimeError(
                    f"Error found in line {line_number} in mapping file. Lines should consist of namespace:pvc-name,namespace:pvc-name only. Namespace and PVCs should adhere to Kubernetes naming conventions."
                )

    # Now let's check if the PVCs passed in the mapping file actually exist in the cluster
    # Let's first split the mapping file into source and target PVCs.
    mapping_file_source_pvcs = []
    mapping_file_target_pvcs = []
    with open(mapping_file, 'r') as file:
        for line in file:
            line = line.strip()
            mapping_file_source_pvc, mapping_file_target_pvc = line.split(',')
            mapping_file_source_pvcs.append(mapping_file_source_pvc)
            mapping_file_target_pvcs.append(mapping_file_target_pvc)
    # Now let's retrieve the PVCs using the retrieve_pvcs() function. We pass no_output as True this time to ensure it doesn't overwrite any existing files
    cluster_source_pvcs = get_bound_pvcs(
        kube_config, source_context
    )
    cluster_target_pvcs = get_bound_pvcs(
        kube_config, target_context
    )

    if cluster_source_pvcs is None:
        raise RuntimeError('Cluster source PVCs is empty')
    if cluster_target_pvcs is None:
        raise RuntimeError('Cluster target PVCs is empty')

    # Now that we have the PVCs existing in the cluster, let's check the data we were given in the mappingfile.
    for item in mapping_file_source_pvcs:
        if item not in cluster_source_pvcs:
            raise RuntimeError(
                f'Source PVC {item} from mapping file was not found in the source cluster'
            )

    for item in mapping_file_target_pvcs:
        if item not in cluster_target_pvcs:
            raise RuntimeError(f'Target PVC {item} was not found in source cluster')

    # Now that we know the file is valid and the PVCs are correct, let's get to work
    # List PVs from source cluster
    config.load_kube_config(config_file=kube_config)
    source_v1 = client.CoreV1Api(
        api_client=config.new_client_from_config(context=source_context)
    )
    source_pvs = source_v1.list_persistent_volume()
    source_pvs = [item for item in source_pvs.items if item.status.phase == 'Bound']

    # List PVs from target cluster
    target_v1 = client.CoreV1Api(
        api_client=config.new_client_from_config(context=target_context)
    )
    target_pvs = target_v1.list_persistent_volume()
    target_pvs = [item for item in target_pvs.items if item.status.phase == 'Bound']

    if source_pvs and target_pvs:
        with open(mapping_file, 'r') as file:
            for line in file:
                line = line.strip()
                # Get the right PV from the source_pvs based on first column in mapping file
                map_src = line.split(',')[0]
                map_src_ns, map_src_name = map_src.split(':')
                source_pv = select_pv_on_pvc(source_pvs, map_src_ns, map_src_name)
                # Get the right PV from the target_pvs based on the second column in mapping file
                map_target = line.split(',')[1]
                map_target_ns, map_target_name = map_target.split(':')
                target_pv = select_pv_on_pvc(target_pvs, map_target_ns, map_target_name)

                if source_pv is None:
                    raise RuntimeError('No PV found for source PVC')
                if target_pv is None:
                    raise RuntimeError('No PV found for target PVC')

                # Let's output these to the user
                # For now, this is much prettier than the final version will be, but it allows for easy debugging/checking
                print(
                    f'Matched source PV of type {"NFS" if source_pv.spec.nfs else "CephRDB"} for PVC {map_src} with target PV of type {"NFS" if target_pv.spec.nfs else "CephRDB"} for PVC {map_target}'
                )
                print(
                    f'Source {"NFS datadir: " if source_pv.spec.nfs else "Ceph volume: " } {source_pv.spec.nfs.path if source_pv.spec.nfs else source_pv.spec.csi.volume_handle}'
                )
                print(
                    f'Target {"NFS datadir: " if target_pv.spec.nfs else "Ceph volume: " } {target_pv.spec.nfs.path if target_pv.spec.nfs else target_pv.spec.csi.volume_handle}'
                )

    else:
        raise RuntimeError('No bound PVs found in source or target cluster')
