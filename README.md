This Python-script allows you to retrieve filepaths for PVs in Kubernetes.  
Requirements:  
- The kubernetes Python library must be installed
- Your user must have permission to list persistent volumes in the source and target clusters

Parameters:  

--kube-config - Specifies a filepath for a kubeconfig-file. Takes precedence over the KUBECONFIG environment variable. If --kube-config is not specified, the script will attempt to retrieve a filepath from the KUBECONFIG environment variable. Should contain necessary contexts for both source and target cluster.

--source-context - Specifies the context to use for the source cluster, in the format user@cluster as specified in the kube-config file

--target-context - Specifies the context to use for the target cluster, in the format user@cluster as specified in the kube-config file  

--retrieve-pvcs - Retrieve PVCs bound to existing PVs in a namespace:pvc-name format for source cluster, target cluster or both and writes this to file  

--mapping-file - Pass a mapping file for the script to use in case PVC-names and namespaces aren't identical between clusters


