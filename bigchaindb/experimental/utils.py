import multiprocessing as mp
from collections import defaultdict

from bigchaindb import BigchainDB
from bigchaindb.tendermint_utils import decode_transaction


RESET = 'reset'
EXIT = 'exit'


class ParallelValidator:
    def __init__(self, number_of_workers=mp.cpu_count()):
        self.number_of_workers = number_of_workers
        self.transaction_index = 0
        self.routing_queues = [mp.Queue() for _ in range(self.number_of_workers)]
        self.workers = []
        self.results_queue = mp.Queue()

    def start(self):
        for routing_queue in self.routing_queues:
            worker = ValidationWorker(routing_queue, self.results_queue)
            process = mp.Process(target=worker.run)
            process.start()
            self.workers.append(process)

    def stop(self):
        for routing_queue in self.routing_queues:
            routing_queue.put(EXIT)

    def validate(self, raw_transaction):
        dict_transaction = decode_transaction(raw_transaction)
        index = int(dict_transaction['id'], 16) % self.number_of_workers
        self.routing_queues[index].put((self.transaction_index, dict_transaction))
        self.transaction_index += 1

    def result(self, timeout=None):
        result_buffer = [None] * self.transaction_index
        for _ in range(self.transaction_index):
            index, transaction = self.results_queue.get(timeout=timeout)
            result_buffer[index] = transaction
        self.transaction_index = 0
        for routing_queue in self.routing_queues:
            routing_queue.put(RESET)
        return result_buffer


class ValidationWorker:
    """Run validation logic in a loop. This Worker is suitable for a Process
    life: no thrills, just a queue to get some values, and a queue to return results.

    Note that a worker is expected to validate multiple transactions in
    multiple rounds, and it needs to keep in memory all transactions already
    validated, until a new round starts. To trigger a new round of validation,
    a ValidationWorker expects a `RESET` message. To exit the infinite loop the
    worker is in, it expects an `EXIT` message.
    """

    def __init__(self, in_queue, results_queue):
        self.in_queue = in_queue
        self.results_queue = results_queue
        self.bigchaindb = BigchainDB()
        self.reset()

    def reset(self):
        # We need a place to store already validated transactions,
        # in case of dependant transactions in the same block.
        # `validated_transactions` maps an `asset_id` with the list
        # of all other transactions sharing the same asset.
        self.validated_transactions = defaultdict(list)

    def validate(self, dict_transaction):
        try:
            asset_id = dict_transaction['asset']['id']
        except KeyError:
            asset_id = dict_transaction['id']

        transaction = self.bigchaindb.is_valid_transaction(
                dict_transaction,
                self.validated_transactions[asset_id])

        if transaction:
            self.validated_transactions[asset_id].append(transaction)
        return transaction

    def run(self):
        while True:
            message = self.in_queue.get()
            if message == RESET:
                self.reset()
            elif message == EXIT:
                return
            else:
                index, transaction = message
                self.results_queue.put((index, self.validate(transaction)))
