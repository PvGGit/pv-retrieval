import argparse
import sys
from functions import *


def main(args):
    kube_config = args.kube_config
    source_context = args.source_context
    target_context = args.target_context
    retrieve_pvcs = args.retrieve_pvcs
    mapping_file = args.mapping_file

    # Retrieve kube-config from environment if it was not passed
    if not kube_config:
        kube_config = retrieve_kubeconfig_env()

    # If no source-context was passed, let's retrieve it from kube_config
    if not source_context:
        source_context = retrieve_source_context(kube_config)

    # If retrieve-pvcs was passed, call the retrieve_pvcs function
    if retrieve_pvcs:
        if (retrieve_pvcs == 'both' or retrieve_pvcs == 'target') and (
            not target_context
        ):
            raise RuntimeError("Can't use options 'both' or 'target' without --target-context set")
        else:
            retrieve_pvcs_from_clusters(
                kube_config, retrieve_pvcs, source_context, target_context
            )

    # If no mapping file was provided and we have a target-context, we match PVs based on identical PVCs (name and ns) with source-cluster leading
    if (not mapping_file) and (target_context):
        retrieve_pvs(kube_config, source_context, target_context)

    # If a mapping file was passed, let's process it
    if mapping_file:
        if target_context:
            retrieve_dirs_from_mapping_file(
                    mapping_file, kube_config, source_context, target_context
                )
        else:
            raise RuntimeError('Please provide --target-context to be used with the mapping file')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--kube-config',
        type=str,
        help='Path to the kubeconfig file. Defaults to the KUBECONFIG environment variable if not specified',
    )
    parser.add_argument(
        '--source-context',
        type=str,
        help='Define the source context to be used. Will use active context if present',
    )
    parser.add_argument(
        '--target-context',
        type=str,
        help='Define the source context to be used. Does not default to active context',
    )
    parser.add_argument(
        '--retrieve-pvcs',
        choices=['source', 'target', 'both'],
        help='Retrieve bound PVCs in cluster. Creates pvc_listing_<source/target>.txt file in the present working directory',
    )
    parser.add_argument('--mapping-file', type=str, help='Path to mapping file')

    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
