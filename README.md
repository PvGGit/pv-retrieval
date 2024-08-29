This Python-script allows you to retrieve filepaths for PVs in Kubernetes. In the present version, it will retrieve NFS paths and CephRDB volumes (if set through CSI). This can help when migrating applications between two clusters.

Requirements:  
- The kubernetes Python library must be installed
- Must be run from a system that can reach source and target cluster
- Your user must have permission to list persistent volumes in the source and target clusters

Parameters:  

--kube-config - Specifies a filepath for a kubeconfig-file. Takes precedence over the KUBECONFIG environment variable. If --kube-config is not specified, the script will attempt to retrieve a filepath from the KUBECONFIG environment variable. Should contain necessary contexts for both source and target cluster.

--source-context - Specifies the context to use for the source cluster, use the name as specified in your kubeconfig file

--target-context - Specifies the context to use for the target cluster, use the name as specified in your kubeconfig file

--retrieve-pvcs - Retrieve PVCs bound to existing PVs in a namespace:pvc-name format for source cluster, target cluster or both and writes this to file so that you can easily use them for a mapping file. 

--mapping-file - Pass a mapping file for the script to use in case PVC-names and namespaces aren't identical between clusters. Expected format is: source-ns:source-pvc,target-ns:target-pvc


