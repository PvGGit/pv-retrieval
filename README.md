This Python-script allows you to retrieve filepaths for PVs in Kubernetes.  

Parameters:  

--source-config - Specifies a filepath for a kubeconfig-file for use with the source Kubernetes cluster. Takes precedence over the KUBECONFIG environment variable. If --source-config is not specified, the script will attempt to retrieve a filepath from the KUBECONFIG environment variable.  

--retrieve-pvs  - Retrieves PVs and their associated filepath from the source cluster. Uses the default namespace. More arguments to follow.
