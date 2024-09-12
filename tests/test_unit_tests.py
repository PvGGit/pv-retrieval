import unittest
import os
from unittest.mock import mock_open, patch
from functions import write_file, retrieve_kubeconfig_env

# Tests for retrieve_kubeconfig_env
class TestRetrieveKubeconfigEnv(unittest.TestCase):
    def test_kubeconfig_set(self):
        with patch.dict(os.environ, {'KUBECONFIG': '/path/kubeconfig'}):
            result = retrieve_kubeconfig_env()
            self.assertEqual(result, 'path/kubeconfig')

    def test_kubeconfig_not_set(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError) as result:
                retrieve_kubeconfig_env()
            self.assertEqual(str(result.exception),
                             'No KUBECONFIG found in environment variables, please specify a valid kube-config file')

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
