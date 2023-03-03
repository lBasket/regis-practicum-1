import sys

sys.path.append('../')
import app

import mock

class Test_Read_Json:
    @mock.patch('app.open')
    def test_read_json_good(self, mock_open):
        '''
        Tests proper functionality given a known, good json.
        '''
        mock_open.return_value.__enter__.return_value.read == '1'
        app.read_json('z')
