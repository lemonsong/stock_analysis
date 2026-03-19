import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Mock dependencies that might be missing
sys.modules['streamlit'] = MagicMock()
sys.modules['pandas'] = MagicMock()
sys.modules['requests'] = MagicMock()
sys.modules['dotenv'] = MagicMock()
sys.modules['plotly'] = MagicMock()
sys.modules['plotly.express'] = MagicMock()

import utils.feishu_helper

class TestFeishuHelperRefactor(unittest.TestCase):

    def test_load_feishu_quarterly_eval_data_uses_token(self):
        with patch('utils.feishu_helper.FEISHU_APP_ID', 'test_app_id'), \
             patch('utils.feishu_helper.FEISHU_APP_KEY', 'test_app_key'), \
             patch('utils.feishu_helper.FEISHU_WIKI_TOKEN', 'test_wiki_token'), \
             patch('utils.feishu_helper.requests.post') as mock_post, \
             patch('utils.feishu_helper.requests.get') as mock_get:

            # Mock token response
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {'tenant_access_token': 'test_access_token'}

            # Mock node response
            mock_get.side_effect = [
                MagicMock(status_code=200, json=lambda: {'data': {'node': {'obj_token': 'test_obj_token'}}}),
                MagicMock(status_code=200, json=lambda: {'data': {'valueRange': {'values': [['symbol', 'quarterly_financial_score'], ['AAPL', 100]]}}})
            ]

            utils.feishu_helper.load_feishu_quarterly_eval_data()

            # Check if first GET call used the correct wiki_token
            node_url = mock_get.call_args_list[0][0][0]
            self.assertIn('token=test_wiki_token', node_url)

    def test_load_feishu_quarterly_eval_data_handles_missing_token(self):
        with patch('utils.feishu_helper.FEISHU_WIKI_TOKEN', None):
            df = utils.feishu_helper.load_feishu_quarterly_eval_data()
            # If FEISHU_WIKI_TOKEN is None, it should return an empty DF (mocked)
            self.assertIsNotNone(df)

    def test_get_feishu_token_and_obj_token_uses_token(self):
        with patch('utils.feishu_helper.FEISHU_APP_ID', 'test_app_id'), \
             patch('utils.feishu_helper.FEISHU_APP_KEY', 'test_app_key'), \
             patch('utils.feishu_helper.FEISHU_WIKI_TOKEN', 'test_wiki_token'), \
             patch('utils.feishu_helper.requests.post') as mock_post, \
             patch('utils.feishu_helper.requests.get') as mock_get:

            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = {'tenant_access_token': 'test_access_token'}

            mock_get.return_value.status_code = 200
            mock_get.return_value.json.return_value = {'data': {'node': {'obj_token': 'test_obj_token'}}}

            token, obj_token = utils.feishu_helper.get_feishu_token_and_obj_token()

            node_url = mock_get.call_args_list[0][0][0]
            self.assertIn('token=test_wiki_token', node_url)
            self.assertEqual(token, 'test_access_token')
            self.assertEqual(obj_token, 'test_obj_token')

if __name__ == '__main__':
    unittest.main()
