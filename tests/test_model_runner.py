import pytest
import concurrent.futures
from app.python_model.model_runner import ModelRunner, ModelSecurityError
from app.python_model.dsl_functions import (
    weighted_avg, schedule_last, schedule_first,
    min_val, max_val, cumulative_sum, median, std_dev,
    _clear_transaction_results, _get_transaction_results
)

def test_dsl_helper_updates():
    # 1. schedule_last / schedule_first with empty schedule returning 0 (merged update)
    assert schedule_last([], "balance") == 0
    assert schedule_first([], "balance") == 0

    # 2. weighted_avg using to_number
    assert weighted_avg([10.0, "20.0"], [1.0, 1.0]) == 15.0
    assert weighted_avg([], []) == 0
    assert weighted_avg([10], [0]) == 0

    # 3. min_val / max_val ignoring None
    assert min_val([10.0, None, 5.0]) == 5.0
    assert min_val([None]) == 0
    assert max_val([10.0, None, 5.0]) == 10.0
    assert max_val([None]) == 0

    # 4. cumulative_sum, median, std_dev using to_number
    assert cumulative_sum(["10", 20]) == [10.0, 30.0]
    assert median(["10", 30, "20"]) == 20.0
    assert std_dev(["10", "10"]) == 0.0


def test_model_runner_basic_execution():
    runner = ModelRunner()
    code = """
from app.python_model.dsl_functions import (
    createTransaction, _clear_transaction_results, _get_transaction_results, _set_current_instrumentid
)
def process_event_data(event_data, raw_event_data, override_postingdate, override_effectivedate):
    _clear_transaction_results()
    _set_current_instrumentid("INS1")
    createTransaction("2026-07-01", "2026-07-01", "Interest", 100.0)
    return _get_transaction_results()
"""
    result = runner.run(python_code=code, event_data=[{"instrumentid": "INS1"}])
    assert result["error"] is None
    assert len(result["transactions"]) == 1
    assert result["transactions"][0]["amount"] == 100.0
    assert result["transactions"][0]["instrumentid"] == "INS1"


def test_model_runner_thread_safety():
    runner = ModelRunner()
    # Runs model runner concurrently in multiple threads.
    # Checks that thread-local isolation prevents transactions from mixing.
    # We use a datetime busy-wait to ensure threads overlap without importing forbidden modules.
    code_template = """
from app.python_model.dsl_functions import (
    createTransaction, _clear_transaction_results, _get_transaction_results, _set_current_instrumentid
)
import datetime
def process_event_data(event_data, raw_event_data, override_postingdate, override_effectivedate):
    _clear_transaction_results()
    _set_current_instrumentid("{inst}")
    
    t0 = datetime.datetime.now()
    while (datetime.datetime.now() - t0).total_seconds() < 0.05:
        pass
        
    createTransaction("2026-07-01", "2026-07-01", "Txn_{inst}", {amount})
    return _get_transaction_results()
"""

    def run_one(inst_id, amt):
        code = code_template.format(inst=inst_id, amount=amt)
        res = runner.run(python_code=code, event_data=[{"instrumentid": inst_id}])
        return res["transactions"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(run_one, f"INST_{i}", i * 10.0): i
            for i in range(1, 6)
        }
        for fut in concurrent.futures.as_completed(futures):
            idx = futures[fut]
            txns = fut.result()
            # Each thread should ONLY see its own created transactions
            assert len(txns) == 1
            assert txns[0]["instrumentid"] == f"INST_{idx}"
            assert txns[0]["amount"] == idx * 10.0


def test_model_runner_security_sandbox():
    runner = ModelRunner()
    
    # 1. Test forbidden imports in template AST compilation
    bad_import_code = """
import socket
def process_event_data(event_data, raw_event_data, override_postingdate, override_effectivedate):
    pass
"""
    with pytest.raises(ValueError) as excinfo:
        runner.compile_template(bad_import_code)
    assert "Disallowed import" in str(excinfo.value)

    # 2. Test forbidden dunder attribute in AST compilation
    bad_dunder_code = """
def process_event_data(event_data, raw_event_data, override_postingdate, override_effectivedate):
    x = object.__subclasses__()
"""
    with pytest.raises(ValueError) as excinfo:
        runner.compile_template(bad_dunder_code)
    assert "not allowed" in str(excinfo.value)

    # 3. Test dynamic import block via __import__ guard at execution time
    safe_builtins = runner._build_safe_builtins()
    
    # Allowed import
    assert safe_builtins['__import__']('json') is not None
    
    # Disallowed import
    with pytest.raises(ImportError) as excinfo:
        safe_builtins['__import__']('subprocess')
    assert "Import of 'subprocess' is not permitted" in str(excinfo.value)
