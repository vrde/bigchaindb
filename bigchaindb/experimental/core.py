from bigchaindb import App
from bigchaindb.experimental.utils import ParallelValidator
from abci.types_pb2 import ResponseDeliverTx


CodeTypeOk = 0


class ParallelValidationApp(App):
    def __init__(self, events_queue, bigchaindb=None):
        super().__init__(events_queue, bigchaindb)
        self.parallel_validator = ParallelValidator()
        self.parallel_validator.start()

    def check_tx(self, raw_transaction):
        # Skip check_tx
        return ResponseCheckTx(code=CodeTypeOk)

    def deliver_tx(self, raw_transaction):
        self.parallel_validator.validate(raw_transaction)
        return ResponseDeliverTx(code=CodeTypeOk)

    def end_block(self, request_end_block):
        result = self.parallel_validator.result(timeout=30)
        for transaction in result:
            if transaction:
                self.block_txn_ids.append(transaction.id)
                self.block_transactions.append(transaction)

        return super().end_block(request_end_block)
