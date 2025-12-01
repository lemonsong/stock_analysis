'''
this is the content of ~/.zipline/extension.py
1. edit this content
2.
# 打开extension.py
$ open  ~/.zipline/extension.py
3. paste to extension.py
'''

# bundle
# Author： Yilin W
import pandas as pd

from zipline.data.bundles import register
from zipline.data.bundles.csvdir import csvdir_equities
from datetime import datetime

start_session = pd.Timestamp('2020-12-01')
# end_session = pd.Timestamp('2025-10-29')

end_session = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))

is_test = False
if is_test:
    is_test_appendix = "_test"
else:
    is_test_appendix = ""
bundle_name = f'cn-daily-bundle{is_test_appendix}'
tframe_folder = f'daily{is_test_appendix}'

register(
    'cn-daily-bundle',
    csvdir_equities(
        ['daily'],
        # change toyour path to the daily folder
        '/Users/yilin/Documents/Projects/stock_analysis/data_dolt',
    ),
    calendar_name='XSHG',  # China mainland stock market calendar
    start_session=start_session,
    end_session=end_session
)
