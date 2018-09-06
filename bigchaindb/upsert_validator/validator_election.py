# Copyright BigchainDB GmbH and BigchainDB contributors
# SPDX-License-Identifier: (Apache-2.0 AND CC-BY-4.0)
# Code is Apache-2.0 and docs are CC-BY-4.0

import base58

from bigchaindb import backend
from bigchaindb.common.exceptions import (InvalidSignature,
                                          MultipleInputsError,
                                          InvalidProposer,
                                          UnequalValidatorSet,
                                          InvalidPowerChange,
                                          DuplicateTransaction)
from bigchaindb.tendermint_utils import key_from_base64
from bigchaindb.common.crypto import (public_key_from_ed25519_key)
from bigchaindb.common.transaction import Transaction
from bigchaindb.common.schema import (_validate_schema,
                                      TX_SCHEMA_VALIDATOR_ELECTION,
                                      TX_SCHEMA_COMMON,
                                      TX_SCHEMA_CREATE)
from . import ValidatorElectionVote
from .validator_utils import (new_validator_set,
                              encode_validator,
                              encode_pk_to_base16,
                              validate_asset_public_key)


class ValidatorElection(Transaction):

    VALIDATOR_ELECTION = 'VALIDATOR_ELECTION'
    # NOTE: this transaction class extends create so the operation inheritence is achieved
    # by renaming CREATE to VALIDATOR_ELECTION
    CREATE = VALIDATOR_ELECTION
    ALLOWED_OPERATIONS = (VALIDATOR_ELECTION,)
    # Election Statuses:
    ONGOING = 'ongoing'
    CONCLUDED = 'concluded'
    INCONCLUSIVE = 'inconclusive'
    ELECTION_THRESHOLD = 2 / 3

    @classmethod
    def get_validator_change(cls, bigchain, height=None):
        """Return the latest change to the validator set

        :return: {
            'height': <block_height>,
            'asset': {
                'height': <block_height>,
                'validators': <validator_set>,
                'election_id': <election_id_that_approved_the_change>
            }
        }
        """
        return bigchain.get_validator_change(height)

    @classmethod
    def get_validators(cls, bigchain, height=None):
        """Return a dictionary of validators with key as `public_key` and
           value as the `voting_power`
        """
        validators = {}
        for validator in bigchain.get_validators(height):
            # NOTE: we assume that Tendermint encodes public key in base64
            public_key = public_key_from_ed25519_key(key_from_base64(validator['public_key']['value']))
            validators[public_key] = validator['voting_power']

        return validators

    @classmethod
    def recipients(cls, bigchain):
        """Convert validator dictionary to a recipient list for `Transaction`"""

        recipients = []
        for public_key, voting_power in cls.get_validators(bigchain).items():
            recipients.append(([public_key], voting_power))

        return recipients

    @classmethod
    def is_same_topology(cls, current_topology, election_topology):
        voters = {}
        for voter in election_topology:
            if len(voter.public_keys) > 1:
                return False

            [public_key] = voter.public_keys
            voting_power = voter.amount
            voters[public_key] = voting_power

        # Check whether the voters and their votes is same to that of the
        # validators and their voting power in the network
        return (current_topology == voters)

    def validate(self, bigchain, current_transactions=[]):
        """Validate election transaction
        For more details refer BEP-21: https://github.com/bigchaindb/BEPs/tree/master/21

        NOTE:
        * A valid election is initiated by an existing validator.

        * A valid election is one where voters are validators and votes are
          alloacted according to the voting power of each validator node.

        Args:
            bigchain (BigchainDB): an instantiated bigchaindb.lib.BigchainDB object.

        Returns:
            ValidatorElection object

        Raises:
            ValidationError: If the election is invalid
        """
        input_conditions = []

        duplicates = any(txn for txn in current_transactions if txn.id == self.id)
        if bigchain.get_transaction(self.id) or duplicates:
            raise DuplicateTransaction('transaction `{}` already exists'
                                       .format(self.id))

        if not self.inputs_valid(input_conditions):
            raise InvalidSignature('Transaction signature is invalid.')

        current_validators = self.get_validators(bigchain)

        # NOTE: Proposer should be a single node
        if len(self.inputs) != 1 or len(self.inputs[0].owners_before) != 1:
            raise MultipleInputsError('`tx_signers` must be a list instance of length one')

        # NOTE: change more than 1/3 of the current power is not allowed
        if self.asset['data']['power'] >= (1/3)*sum(current_validators.values()):
            raise InvalidPowerChange('`power` change must be less than 1/3 of total power')

        # NOTE: Check if the proposer is a validator.
        [election_initiator_node_pub_key] = self.inputs[0].owners_before
        if election_initiator_node_pub_key not in current_validators.keys():
            raise InvalidProposer('Public key is not a part of the validator set')

        # NOTE: Check if all validators have been assigned votes equal to their voting power
        if not self.is_same_topology(current_validators, self.outputs):
            raise UnequalValidatorSet('Validator set much be exactly same to the outputs of election')

        return self

    @classmethod
    def generate(cls, initiator, voters, election_data, metadata=None):
        (inputs, outputs) = cls.validate_create(initiator, voters, election_data, metadata)
        election = cls(cls.VALIDATOR_ELECTION, {'data': election_data}, inputs, outputs, metadata)
        cls.validate_schema(election.to_dict(), skip_id=True)
        return election

    @classmethod
    def validate_schema(cls, tx, skip_id=False):
        """Validate the validator election transaction. Since `VALIDATOR_ELECTION` extends `CREATE`
           transaction, all the validations for `CREATE` transaction should be inherited
        """
        if not skip_id:
            cls.validate_id(tx)
        _validate_schema(TX_SCHEMA_COMMON, tx)
        _validate_schema(TX_SCHEMA_CREATE, tx)
        _validate_schema(TX_SCHEMA_VALIDATOR_ELECTION, tx)
        validate_asset_public_key(tx['asset']['data']['public_key'])

    @classmethod
    def create(cls, tx_signers, recipients, metadata=None, asset=None):
        raise NotImplementedError

    @classmethod
    def transfer(cls, tx_signers, recipients, metadata=None, asset=None):
        raise NotImplementedError

    @classmethod
    def to_public_key(cls, election_id):
        return base58.b58encode(bytes.fromhex(election_id)).decode()

    @classmethod
    def count_votes(cls, election_pk, transactions, getter=getattr):
        votes = 0
        for txn in transactions:
            if getter(txn, 'operation') == 'VALIDATOR_ELECTION_VOTE':
                for output in getter(txn, 'outputs'):
                    # NOTE: We enforce that a valid vote to election id will have only
                    # election_pk in the output public keys, including any other public key
                    # along with election_pk will lead to vote being not considered valid.
                    if len(getter(output, 'public_keys')) == 1 and [election_pk] == getter(output, 'public_keys'):
                        votes = votes + int(getter(output, 'amount'))
        return votes

    def get_commited_votes(self, bigchain, election_pk=None):
        if election_pk is None:
            election_pk = self.to_public_key(self.id)
        txns = list(backend.query.get_asset_tokens_for_public_key(bigchain.connection,
                                                                  self.id,
                                                                  election_pk))
        return self.count_votes(election_pk, txns, dict.get)

    @classmethod
    def has_concluded(cls, bigchain, election_id, current_votes=[], height=None):
        """Check if the given `election_id` can be concluded or not
        NOTE:
        * Election is concluded iff the current validator set is exactly equal
          to the validator set encoded in election outputs
        * Election can concluded only if the current votes achieves a supermajority
        """
        election = bigchain.get_transaction(election_id)

        if election:
            election_pk = election.to_public_key(election.id)
            votes_commited = election.get_commited_votes(bigchain, election_pk)
            votes_current = election.count_votes(election_pk, current_votes)
            current_validators = election.get_validators(bigchain, height)

            if election.is_same_topology(current_validators, election.outputs):
                total_votes = sum(current_validators.values())
                if (votes_commited < (2/3)*total_votes) and \
                   (votes_commited + votes_current >= (2/3)*total_votes):
                    return election
        return False

    @classmethod
    def get_validator_update(cls, bigchain, new_height, txns):
        votes = {}
        for txn in txns:
            if not isinstance(txn, ValidatorElectionVote):
                continue

            election_id = txn.asset['id']
            election_votes = votes.get(election_id, [])
            election_votes.append(txn)
            votes[election_id] = election_votes

            election = cls.has_concluded(bigchain, election_id, election_votes, new_height)
            # Once an election concludes any other conclusion for the same
            # or any other election is invalidated
            if election:
                # The new validator set comes into effect from height = new_height+1
                validator_updates = [election.asset['data']]
                curr_validator_set = bigchain.get_validators(new_height)
                updated_validator_set = new_validator_set(curr_validator_set,
                                                          validator_updates)

                updated_validator_set = [v for v in updated_validator_set if v['voting_power'] > 0]
                bigchain.store_validator_set(new_height+1, updated_validator_set, election.id)

                validator16 = encode_pk_to_base16(election.asset['data'])
                return [encode_validator(validator16)]

        return []

    def get_validator_update_by_election_id(self, election_id, bigchain):
        result = bigchain.get_validators_by_election_id(election_id)
        return result

    def get_status(self, bigchain):
        concluded = self.get_validator_update_by_election_id(self.id, bigchain)
        if concluded:
            return self.CONCLUDED

        latest_change = self.get_validator_change(bigchain)
        latest_change_height = latest_change['height']
        election_height = bigchain.get_block_containing_tx(self.id)[0]

        if latest_change_height >= election_height:
            return self.INCONCLUSIVE
        else:
            return self.ONGOING
