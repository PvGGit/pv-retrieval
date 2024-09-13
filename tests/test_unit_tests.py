import unittest
import os
from unittest.mock import mock_open, patch, MagicMock
from functions import write_file, retrieve_kubeconfig_env, list_pvs
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
        mock_pv_list = MagicMock(spec=V1PersistentVolumeList)  # Mock V1PersistentVolumeList object
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
