import unittest
import os
from unittest.mock import mock_open, patch, MagicMock
from functions import write_file, retrieve_kubeconfig_env, list_pvs, get_bound_pvcs, retrieve_pvcs_from_clusters, retrieve_source_context
from kubernetes.client.models import V1PersistentVolumeList, V1PersistentVolume

# Tests for retrieve_kubeconfig_env
class TestRetrieveKubeconfigEnv(unittest.TestCase):
    def test_kubeconfig_set(self):
        with patch.dict(os.environ, {'KUBECONFIG': '/path/kubeconfig'}):
            result = retrieve_kubeconfig_env()
            self.assertEqual(result, '/path/kubeconfig')

    def test_kubeconfig_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as result:
                retrieve_kubeconfig_env()
            self.assertEqual(str(result.exception),
                             'No KUBECONFIG found in environment variables, please specify a valid kube-config file')

# Tests for list_pvs
class TestListPVs(unittest.TestCase):
    @patch('kubernetes.config.new_client_from_config')
    @patch('kubernetes.config.load_kube_config')
    @patch('kubernetes.client.CoreV1Api')
    def test_list_pvs(self, mock_core_v1_api, mock_load_kube_config, mock_new_client_from_config):
        mock_pv_list = MagicMock(spec=V1PersistentVolumeList)
        mock_core_v1_api_instance = mock_core_v1_api.return_value
        mock_core_v1_api_instance.list_persistent_volume.return_value = mock_pv_list

        mock_load_kube_config.return_value = None

        mock_new_client_from_config.return_value = MagicMock()

        kube_config = '/path/kubeconfig'
        context = 'context'

        result = list_pvs(kube_config, context)

        mock_load_kube_config.assert_called_once_with(config_file=kube_config)
        mock_new_client_from_config.assert_called_once_with(context=context)
        mock_core_v1_api.assert_called_once_with(api_client=mock_new_client_from_config.return_value)
        mock_core_v1_api_instance.list_persistent_volume.assert_called_once()
        self.assertEqual(result, mock_pv_list)

# Test for get_bound_pvcs
class TestGetBoundPvcs(unittest.TestCase):
    @patch('kubernetes.client.CoreV1Api')
    @patch('kubernetes.config.load_kube_config')
    @patch('kubernetes.config.new_client_from_config')
    def test_get_bound_pvcs(self, mock_new_client, mock_load_kube_config, mock_core_v1_api):
        mock_v1_api_instance = MagicMock()
        mock_core_v1_api.return_value = mock_v1_api_instance

        mock_volume_bound = MagicMock()
        mock_volume_bound.status.phase = 'Bound'
        mock_volume_bound.spec.claim_ref.namespace = 'default'
        mock_volume_bound.spec.claim_ref.name = 'test-pvc'

        mock_volume_unbound = MagicMock()
        mock_volume_unbound.status.phase = 'Pending'

        mock_v1_api_instance.list_persistent_volume.return_value.items = [
            mock_volume_bound, mock_volume_unbound
        ]

        result = get_bound_pvcs('kube_config', 'context')

        mock_load_kube_config.assert_called_once_with(config_file='kube_config')
        mock_new_client.assert_called_once_with(context='context')
        mock_v1_api_instance.list_persistent_volume.assert_called_once()

        self.assertEqual(result, ['default:test-pvc'])

# Test for retrieve_pvcs_from_cluster
class TestRetrievePVCsFromCluster(unittest.TestCase):
    @patch('functions.write_file')
    @patch('functions.get_bound_pvcs')
    @patch('kubernetes.config.load_kube_config')
    @patch('builtins.print')
    def test_retrieve_pvcs_from_source(self, mock_print, mock_load_kube_config, mock_get_bound_pvcs, mock_write_file):
        mock_get_bound_pvcs.return_value = ['default:test-pvc']

        retrieve_pvcs_from_clusters('kube_config', 'source', 'source_context', 'target_context')

        mock_load_kube_config.assert_called_once_with(config_file='kube_config')
        mock_get_bound_pvcs.assert_called_once_with('kube_config', 'source_context')
        mock_write_file.assert_called_once_with(['default:test-pvc'], 'source_pvcs.txt')
        mock_print.assert_called_once_with('PVCs for source-context source_context written to source_pvcs.txt')

    @patch('functions.write_file')
    @patch('functions.get_bound_pvcs')
    @patch('kubernetes.config.load_kube_config')
    @patch('builtins.print')
    def test_retrieve_pvcs_from_target(self, mock_print, mock_load_kube_config, mock_get_bound_pvcs, mock_write_file):
        mock_get_bound_pvcs.return_value = ['default:test-pvc']

        retrieve_pvcs_from_clusters('kube_config', 'target', 'source_context', 'target_context')

        mock_load_kube_config.assert_called_once_with(config_file='kube_config')
        mock_get_bound_pvcs.assert_called_once_with('kube_config', 'target_context')
        mock_write_file.assert_called_once_with(['default:test-pvc'], 'target_pvcs.txt')
        mock_print.assert_called_once_with('PVCs for target-context target_context written to target_pvcs.txt')

    @patch('functions.write_file')
    @patch('functions.get_bound_pvcs')
    @patch('kubernetes.config.load_kube_config')
    @patch('builtins.print')
    def test_retrieve_pvcs_from_both(self, mock_print, mock_load_kube_config, mock_get_bound_pvcs, mock_write_file):
        mock_get_bound_pvcs.side_effect = [['default:source-pvc'], ['default:target-pvc']]

        retrieve_pvcs_from_clusters('kube_config', 'both', 'source_context', 'target_context')

        mock_load_kube_config.assert_called_once_with(config_file='kube_config')
        self.assertEqual(mock_get_bound_pvcs.call_count, 2)
        mock_get_bound_pvcs.assert_any_call('kube_config', 'source_context')
        mock_get_bound_pvcs.assert_any_call('kube_config', 'target_context')
        mock_write_file.assert_any_call(['default:source-pvc'], 'source_pvcs.txt')
        mock_write_file.assert_any_call(['default:target-pvc'], 'target_pvcs.txt')
        self.assertEqual(mock_print.call_count, 2)
        mock_print.assert_any_call('PVCs for source-context source_context written to source_pvcs.txt')
        mock_print.assert_any_call('PVCs for target-context target_context written to target_pvcs.txt')


# Test for write_file function
class TestWriteFile(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open)
    def test_write_file(self, mock_file):
        """
        Test that write_file writes the correct content to the file.
        """

        volumes = ['volume1', 'volume2', 'volume3']
        file_path = 'fake_file.txt'

        write_file(volumes, file_path)

        mock_file.assert_called_with(file_path, 'w')

        handle = mock_file()
        handle.write.assert_any_call('volume1\n')
        handle.write.assert_any_call('volume2\n')
        handle.write.assert_any_call('volume3\n')

class TestRetrieveSourceContext(unittest.TestCase):
    @patch('kubernetes.config.load_kube_config')
    @patch('kubernetes.config.list_kube_config_contexts')
    def test_retrieve_source_context(self, mock_list_kube_config_contexts, mock_load_kube_config):
        mock_list_kube_config_contexts.return_value = (None, {'name': 'active-context'})

        active_context = retrieve_source_context('kube_config')

        mock_load_kube_config.assert_called_once_with(config_file='kube_config')
        mock_list_kube_config_contexts.assert_called_once()
        self.assertEqual(active_context, 'active-context')
