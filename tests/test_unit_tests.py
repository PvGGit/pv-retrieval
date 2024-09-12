import unittest
from unittest.mock import mock_open, patch
from functions import write_file  # Import the function you're testing

# Test for write_file function
class TestWriteFile(unittest.TestCase):
    @patch('builtins.open', new_callable=mock_open)
    def test_write_file(self, mock_file):
        """
        Test that write_file writes the correct content to the file.
        """

        # Arrange: Define the input data
        volumes = ['volume1', 'volume2', 'volume3']
        file_path = 'fake_file.txt'

        # Act: Call the function with the mocked file
        write_file(volumes, file_path)

        # Check that the file was opened in write mode
        mock_file.assert_called_with(file_path, 'w')

        # Assert: Check that the file was written to with the expected content
        mock_file.write.assert_any_call('volume1\n')
        mock_file.write.assert_any_call('volume2\n')
        mock_file.write.assert_any_call('volume3\n')
