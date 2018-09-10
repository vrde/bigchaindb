import pytest

from bigchaindb.common.crypto import generate_key_pair
from bigchaindb.models import Transaction


pytestmark = pytest.mark.tendermint


def generate_create_and_transfer(keypair=None):
    if not keypair:
        keypair = generate_key_pair()
    priv_key, pub_key = keypair
    create_tx = Transaction.create([pub_key], [([pub_key], 10)]).sign([priv_key])
    transfer_tx = Transaction.transfer(
            create_tx.to_inputs(),
            [([pub_key], 10)],
            asset_id=create_tx.id).sign([priv_key])
    return create_tx, transfer_tx


def test_validation_worker_process_multiple_transactions(b):
    import multiprocessing as mp
    from bigchaindb.parallel_validation import ValidationWorker, RESET, EXIT

    keypair = generate_key_pair()
    create_tx, transfer_tx = generate_create_and_transfer(keypair)
    double_spend = Transaction.transfer(
            create_tx.to_inputs(),
            [([keypair.public_key], 10)],
            asset_id=create_tx.id).sign([keypair.private_key])

    in_queue, results_queue = mp.Queue(), mp.Queue()
    vw = ValidationWorker(in_queue, results_queue)

    # Note: in the following instructions, the worker will encounter two
    # `RESET` messages, and an `EXIT` message. When a worker processes a
    # `RESET` message, it forgets all transactions it has validated. This allow
    # us to re-validate the same transactions. This won't happen in real life,
    # but it's quite handy to check if the worker actually forgot about the
    # past transactions (if not, it will return `False` because the
    # transactions look like a double spend).
    # `EXIT` makes the worker to stop the infinite loop.
    in_queue.put((0, create_tx.to_dict()))
    in_queue.put((10, transfer_tx.to_dict()))
    in_queue.put((20, double_spend.to_dict()))
    in_queue.put(RESET)
    in_queue.put((0, create_tx.to_dict()))
    in_queue.put((5, transfer_tx.to_dict()))
    in_queue.put(RESET)
    in_queue.put((20, create_tx.to_dict()))
    in_queue.put((25, double_spend.to_dict()))
    in_queue.put((30, transfer_tx.to_dict()))
    in_queue.put(EXIT)

    vw.run()

    assert results_queue.get() == (0, create_tx)
    assert results_queue.get() == (10, transfer_tx)
    assert results_queue.get() == (20, False)
    assert results_queue.get() == (0, create_tx)
    assert results_queue.get() == (5, transfer_tx)
    assert results_queue.get() == (20, create_tx)
    assert results_queue.get() == (25, double_spend)
    assert results_queue.get() == (30, False)


def test_parallel_validator_routes_transactions_correctly(b, monkeypatch):
    import os
    import multiprocessing as mp
    from json import dumps
    from bigchaindb.parallel_validation import ParallelValidator

    # We want to make sure that the load is distributed across all workers.
    # Since introspection on an object running on a different process is
    # difficult, we create an additional queue where every worker can emit its
    # PID every time validation is called.
    validation_called_by = mp.Queue()

    # Validate is now a passthrough, and every time it is called it will emit
    # the PID of its worker to the designated queue.
    def validate(self, dict_transaction):
        validation_called_by.put(os.getpid())
        return dict_transaction

    monkeypatch.setattr(
        'bigchaindb.parallel_validation.ValidationWorker.validate',
        validate)

    # Transaction routing uses the `id` of the transaction. This test strips
    # down a transaction to just its `id`. We have two workers, so even ids
    # will be processed by one worker, odd ids by the other.
    transactions = [{'id': '0'}, {'id': '1'}, {'id': '2'}, {'id': '3'}]

    pv = ParallelValidator(number_of_workers=2)
    pv.start()

    # ParallelValidator is instantiated once, and then used several times.
    # Here we simulate this scenario by running it an arbitrary number of
    # times.
    # Note that the `ParallelValidator.result` call resets the object, and
    # makes it ready to validate a new set of transactions.
    for _ in range(2):
        for transaction in transactions:
            pv.validate(dumps(transaction).encode('utf8'))
        # We expect the result to have the same order as the input data.
        assert pv.result(timeout=1) == transactions
        pids = set()
        for _ in transactions:
            pids.add(validation_called_by.get())
        assert len(pids) == 2

    pv.stop()
