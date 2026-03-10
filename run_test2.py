import sys
sys.path.append('.')
import pages
import importlib.util
spec = importlib.util.spec_from_file_location("buy_signals", "pages/2_💡Buy_Signals.py")
buy_signals = importlib.util.module_from_spec(spec)
sys.modules["buy_signals"] = buy_signals
spec.loader.exec_module(buy_signals)

df = buy_signals.load_decision_data()
print("DF is None:", df is None)
if df is not None:
    print(df.head())
