import unittest
import os
from unittest.mock import mock_open, patch, MagicMock
from functions import write_file, retrieve_kubeconfig_env, list_pvs, get_bound_pvcs, retrieve_pvcs_from_clusters, retrieve_source_context, extract_values_from_pvs, retrieve_pvs, match_pvs
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

class ExtractValuesFromPVs(unittest.TestCase):
    def test_extract_values_from_pvs(self):
        pv1 = MagicMock()
        pv1.metadata.name = 'pv1'
        pv1.status.phase = 'Bound'
        pv1.spec.claim_ref.name = 'pvc1'
        pv1.spec.claim_ref.namespace = 'default'
        pv1.spec.nfs.path = '/nfs/path1'
        pv1.spec.csi = None

        pv2 = MagicMock()
        pv2.metadata.name = 'pv2'
        pv2.status.phase = 'Bound'
        pv2.spec.claim_ref.name = 'pvc2'
        pv2.spec.claim_ref.namespace = 'test'
        pv2.spec.nfs = None
        pv2.spec.csi.volume_handle = 'cephrdb-vol1'

        pv3 = MagicMock()
        pv3.metadata.name = 'pv3'
        pv3.status.phase = 'Available'
        pv3.spec.claim_ref.name = 'pvc3'
        pv3.spec.claim_ref.namespace = 'default'
        pv3.spec.nfs.path = '/nfs/path2'
        pv3.spec.csi = None

        pv_list = MagicMock()
        pv_list.items = [pv1, pv2, pv3]

        result = extract_values_from_pvs(pv_list)

        expected_output = [
            {
                'name': 'pv1',
                'pvc_name': 'pvc1',
                'pvc_ns': 'default',
                'data_dir': '/nfs/path1'
            },
            {
                'name': 'pv2',
                'pvc_name': 'pvc2',
                'pvc_ns': 'test',
                'data_dir': 'cephrdb-vol1'
            }
        ]

        self.assertEqual(result, expected_output)

class TestRetrievePVs(unittest.TestCase):
    @patch('functions.list_pvs')
    @patch('functions.extract_values_from_pvs')
    @patch('functions.match_pvs')
    def test_retrieve_pvs_succesful_match(self, mock_match_pvs, mock_extract_values_from_pvs, mock_list_pvs):
        mock_list_pvs.side_effect = [
            MagicMock(),
            MagicMock()
        ]

        mock_extract_values_from_pvs.side_effect = [
            [{'name':'pv1', 'pvc_name':'pvc1', 'pvc_ns':'default', 'data_dir':'/nfs/path'}],
            [{'name':'pv2', 'pvc_name':'pvc2', 'pvc_ns':'default', 'data_dir':'cephrdb-vol1'}]
        ]

        retrieve_pvs('kube_config', 'source-context', 'target-context')

        self.assertEqual(mock_list_pvs.call_count, 2)
        self.assertEqual(mock_extract_values_from_pvs.call_count, 2)
        mock_match_pvs.assert_called_once_with(
            [{'name':'pv1', 'pvc_name':'pvc1', 'pvc_ns':'default', 'data_dir':'/nfs/path'}],
            [{'name':'pv2', 'pvc_name':'pvc2', 'pvc_ns':'default', 'data_dir':'cephrdb-vol1'}]
        )

    @patch('functions.list_pvs')
    def test_retrieve_pvs_no_source_pvs(self, mock_list_pvs):
        mock_list_pvs.side_effect = [
            [],
            MagicMock()
        ]

        with self.assertRaises(RuntimeError) as context:
            retrieve_pvs('kube_config', 'source-context', 'target-context')

        self.assertIn('No PersistentVolumes were found in source context', str(context.exception))

    @patch('functions.list_pvs')
    def test_retrieve_no_target_pvs(self, mock_list_pvs):
        mock_list_pvs.side_effect = [
            MagicMock(),
            []
        ]

        with self.assertRaises(RuntimeError) as context:
            retrieve_pvs('kube_config', 'source-context', 'target-context')

        self.assertIn('No PersistentVolumes were found in target context', str(context.exception))

class TestMatchPVCs(unittest.TestCase):

    @patch('builtins.print')
    def test_match_pvcs_succesful(self, mock_print):
        source_pvs = [
            {'name': 'pv1', 'pvc_name': 'pvc1', 'pvc_ns': 'default', 'data_dir': '/nfs/path1'},
            {'name': 'pv2', 'pvc_name': 'pvc2', 'pvc_ns': 'default', 'data_dir': '/nfs/path2'}
        ]
        target_pvs = [
            {'name': 'target-pv1', 'pvc_name': 'pvc1', 'pvc_ns': 'default', 'data_dir': '/nfs/path1'},
            {'name': 'target-pv2', 'pvc_name': 'pvc2', 'pvc_ns': 'default', 'data_dir': '/nfs/path2'}
        ]

        match_pvs(source_pvs, target_pvs)

        self.assertEqual(mock_print.call_count, 4)
        mock_print.assert_any_call("Source PV called pv1 matches with target PV called target-pv1")
        mock_print.assert_any_call("Data dirs: /nfs/path1 /nfs/path1")
        mock_print.assert_any_call("Source PV called pv2 matches with target PV called target-pv2")
        mock_print.assert_any_call("Data dirs: /nfs/path2 /nfs/path2")

    def test_match_pvcs_no_match(self):
        source_pvs = [
            {'name': 'pv1', 'pvc_name': 'pvc1', 'pvc_ns': 'default', 'data_dir': '/nfs/path1'}
        ]

        target_pvs = [
            {'name': 'target-pv2', 'pvc_name': 'pvc2', 'pvc_ns': 'default', 'data_dir': '/nfs/path2'}
        ]

        with self.assertRaises(RuntimeError) as context:
            match_pvs(source_pvs, target_pvs)

        self.assertIn('PVCs in source and target cluster are not identical', str(context.exception))
