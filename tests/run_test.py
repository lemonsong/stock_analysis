import importlib.util
import sys
spec = importlib.util.spec_from_file_location("buy_signals", "../pages/2_💡Buy_Signals.py")
buy_signals = importlib.util.module_from_spec(spec)
sys.modules["buy_signals"] = buy_signals
spec.loader.exec_module(buy_signals)

try:
    df = buy_signals.load_decision_data()
    print("Loaded df:", df is not None)
except Exception as e:
    import traceback
    traceback.print_exc()
