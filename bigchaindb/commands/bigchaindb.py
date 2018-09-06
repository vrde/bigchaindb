# Copyright BigchainDB GmbH and BigchainDB contributors
# SPDX-License-Identifier: (Apache-2.0 AND CC-BY-4.0)
# Code is Apache-2.0 and docs are CC-BY-4.0

"""Implementation of the `bigchaindb` command,
the command-line interface (CLI) for BigchainDB Server.
"""

import os
import logging
import argparse
import copy
import json
import sys

from bigchaindb.utils import load_node_key
from bigchaindb.common.exceptions import (DatabaseAlreadyExists,
                                          DatabaseDoesNotExist,
                                          ValidationError)
import bigchaindb
from bigchaindb import (backend, ValidatorElection,
                        BigchainDB, ValidatorElectionVote)
from bigchaindb.backend import schema
from bigchaindb.backend import query
from bigchaindb.backend.query import PRE_COMMIT_ID
from bigchaindb.commands import utils
from bigchaindb.commands.utils import (configure_bigchaindb,
                                       input_on_stderr)
from bigchaindb.log import setup_logging
from bigchaindb.tendermint_utils import public_key_from_base64, public_key_to_base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Note about printing:
#   We try to print to stdout for results of a command that may be useful to
#   someone (or another program). Strictly informational text, or errors,
#   should be printed to stderr.


@configure_bigchaindb
def run_show_config(args):
    """Show the current configuration"""
    # TODO Proposal: remove the "hidden" configuration. Only show config. If
    # the system needs to be configured, then display information on how to
    # configure the system.
    config = copy.deepcopy(bigchaindb.config)
    del config['CONFIGURED']
    print(json.dumps(config, indent=4, sort_keys=True))


@configure_bigchaindb
def run_configure(args):
    """Run a script to configure the current node."""
    config_path = args.config or bigchaindb.config_utils.CONFIG_DEFAULT_PATH

    config_file_exists = False
    # if the config path is `-` then it's stdout
    if config_path != '-':
        config_file_exists = os.path.exists(config_path)

    if config_file_exists and not args.yes:
        want = input_on_stderr('Config file `{}` exists, do you want to '
                               'override it? (cannot be undone) [y/N]: '.format(config_path))
        if want != 'y':
            return

    conf = copy.deepcopy(bigchaindb.config)

    # select the correct config defaults based on the backend
    print('Generating default configuration for backend {}'
          .format(args.backend), file=sys.stderr)
    database_keys = bigchaindb._database_keys_map[args.backend]
    conf['database'] = bigchaindb._database_map[args.backend]

    if not args.yes:
        for key in ('bind', ):
            val = conf['server'][key]
            conf['server'][key] = input_on_stderr('API Server {}? (default `{}`): '.format(key, val), val)

        for key in ('scheme', 'host', 'port'):
            val = conf['wsserver'][key]
            conf['wsserver'][key] = input_on_stderr('WebSocket Server {}? (default `{}`): '.format(key, val), val)

        for key in database_keys:
            val = conf['database'][key]
            conf['database'][key] = input_on_stderr('Database {}? (default `{}`): '.format(key, val), val)

        for key in ('host', 'port'):
            val = conf['tendermint'][key]
            conf['tendermint'][key] = input_on_stderr('Tendermint {}? (default `{}`)'.format(key, val), val)

    if config_path != '-':
        bigchaindb.config_utils.write_config(conf, config_path)
    else:
        print(json.dumps(conf, indent=4, sort_keys=True))
    print('Configuration written to {}'.format(config_path), file=sys.stderr)
    print('Ready to go!', file=sys.stderr)


@configure_bigchaindb
def run_upsert_validator(args):
    """Initiate and manage elections to change the validator set"""

    b = BigchainDB()

    # Call the function specified by args.action, as defined above
    globals()[f'run_upsert_validator_{args.action}'](args, b)


def run_upsert_validator_new(args, bigchain):
    """Initiates an election to add/update/remove a validator to an existing BigchainDB network

    :param args: dict
        args = {
        'public_key': the public key of the proposed peer, (str)
        'power': the proposed validator power for the new peer, (str)
        'node_id': the node_id of the new peer (str)
        'sk': the path to the private key of the node calling the election (str)
        }
    :param bigchain: an instance of BigchainDB
    :return: election_id or `False` in case of failure
    """

    new_validator = {
        'public_key': {'value': public_key_from_base64(args.public_key),
                       'type': 'ed25519-base16'},
        'power': args.power,
        'node_id': args.node_id
    }

    try:
        key = load_node_key(args.sk)
        voters = ValidatorElection.recipients(bigchain)
        election = ValidatorElection.generate([key.public_key],
                                              voters,
                                              new_validator, None).sign([key.private_key])
        election.validate(bigchain)
    except ValidationError as e:
        logger.error(e)
        return False
    except FileNotFoundError as fd_404:
        logger.error(fd_404)
        return False

    resp = bigchain.write_transaction(election, 'broadcast_tx_commit')
    if resp == (202, ''):
        logger.info('[SUCCESS] Submitted proposal with id: {}'.format(election.id))
        return election.id
    else:
        logger.error('Failed to commit election proposal')
        return False


def run_upsert_validator_approve(args, bigchain):
    """Approve an election to add/update/remove a validator to an existing BigchainDB network

    :param args: dict
        args = {
        'election_id': the election_id of the election (str)
        'sk': the path to the private key of the signer (str)
        }
    :param bigchain: an instance of BigchainDB
    :return: success log message or `False` in case of error
    """

    key = load_node_key(args.sk)
    tx = bigchain.get_transaction(args.election_id)
    voting_powers = [v.amount for v in tx.outputs if key.public_key in v.public_keys]
    if len(voting_powers) > 0:
        voting_power = voting_powers[0]
    else:
        logger.error('The key you provided does not match any of the eligible voters in this election.')
        return False

    inputs = [i for i in tx.to_inputs() if key.public_key in i.owners_before]
    election_pub_key = ValidatorElection.to_public_key(tx.id)
    approval = ValidatorElectionVote.generate(inputs,
                                              [([election_pub_key], voting_power)],
                                              tx.id).sign([key.private_key])
    approval.validate(bigchain)

    resp = bigchain.write_transaction(approval, 'broadcast_tx_commit')

    if resp == (202, ''):
        logger.info('[SUCCESS] Your vote has been submitted')
        return approval.id
    else:
        logger.error('Failed to commit vote')
        return False


def run_upsert_validator_show(args, bigchain):
    """Retrieves information about an upsert-validator election

    :param args: dict
        args = {
        'election_id': the transaction_id for an election (str)
        }
    :param bigchain: an instance of BigchainDB
    """

    election = bigchain.get_transaction(args.election_id)
    if not election:
        logger.error(f'No election found with election_id {args.election_id}')
        return

    new_validator = election.asset['data']

    public_key = public_key_to_base64(new_validator['public_key']['value'])
    power = new_validator['power']
    node_id = new_validator['node_id']
    status = election.get_status(bigchain)

    response = f'public_key={public_key}\npower={power}\nnode_id={node_id}\nstatus={status}'

    logger.info(response)

    return response


def _run_init():
    bdb = bigchaindb.BigchainDB()

    schema.init_database(connection=bdb.connection)


@configure_bigchaindb
def run_init(args):
    """Initialize the database"""
    # TODO Provide mechanism to:
    # 1. prompt the user to inquire whether they wish to drop the db
    # 2. force the init, (e.g., via -f flag)
    try:
        _run_init()
    except DatabaseAlreadyExists:
        print('The database already exists.', file=sys.stderr)
        print('If you wish to re-initialize it, first drop it.', file=sys.stderr)


@configure_bigchaindb
def run_drop(args):
    """Drop the database"""
    dbname = bigchaindb.config['database']['name']

    if not args.yes:
        response = input_on_stderr('Do you want to drop `{}` database? [y/n]: '.format(dbname))
        if response != 'y':
            return

    conn = backend.connect()
    dbname = bigchaindb.config['database']['name']
    try:
        schema.drop_database(conn, dbname)
    except DatabaseDoesNotExist:
        print("Cannot drop '{name}'. The database does not exist.".format(name=dbname), file=sys.stderr)


def run_recover(b):
    pre_commit = query.get_pre_commit_state(b.connection, PRE_COMMIT_ID)

    # Initially the pre-commit collection would be empty
    if pre_commit:
        latest_block = query.get_latest_block(b.connection)

        # NOTE: the pre-commit state can only be ahead of the commited state
        # by 1 block
        if latest_block and (latest_block['height'] < pre_commit['height']):
            query.delete_transactions(b.connection, pre_commit['transactions'])


@configure_bigchaindb
def run_start(args):
    """Start the processes to run the node"""

    # Configure Logging
    setup_logging()

    logger.info('BigchainDB Version %s', bigchaindb.__version__)
    run_recover(bigchaindb.lib.BigchainDB())

    try:
        if not args.skip_initialize_database:
            logger.info('Initializing database')
            _run_init()
    except DatabaseAlreadyExists:
        pass

    logger.info('Starting BigchainDB main process.')
    from bigchaindb.start import start
    start()


def create_parser():
    parser = argparse.ArgumentParser(
        description='Control your BigchainDB node.',
        parents=[utils.base_parser])

    # all the commands are contained in the subparsers object,
    # the command selected by the user will be stored in `args.command`
    # that is used by the `main` function to select which other
    # function to call.
    subparsers = parser.add_subparsers(title='Commands',
                                       dest='command')

    # parser for writing a config file
    config_parser = subparsers.add_parser('configure',
                                          help='Prepare the config file.')

    config_parser.add_argument('backend',
                               choices=['localmongodb'],
                               default='localmongodb',
                               const='localmongodb',
                               nargs='?',
                               help='The backend to use. It can only be '
                               '"localmongodb", currently.')

    # parser for managing validator elections
    validator_parser = subparsers.add_parser('upsert-validator',
                                             help='Add/update/delete a validator.')

    validator_subparser = validator_parser.add_subparsers(title='Action',
                                                          dest='action')

    new_election_parser = validator_subparser.add_parser('new',
                                                         help='Calls a new election.')

    new_election_parser.add_argument('public_key',
                                     help='Public key of the validator to be added/updated/removed.')

    new_election_parser.add_argument('power',
                                     type=int,
                                     help='The proposed power for the validator. '
                                          'Setting to 0 will remove the validator.')

    new_election_parser.add_argument('node_id',
                                     help='The node_id of the validator.')

    new_election_parser.add_argument('--private-key',
                                     dest='sk',
                                     help='Path to the private key of the election initiator.')

    approve_election_parser = validator_subparser.add_parser('approve',
                                                             help='Approve the election.')
    approve_election_parser.add_argument('election_id',
                                         help='The election_id of the election.')
    approve_election_parser.add_argument('--private-key',
                                         dest='sk',
                                         help='Path to the private key of the election initiator.')

    show_election_parser = validator_subparser.add_parser('show',
                                                          help='Provides information about an election.')

    show_election_parser.add_argument('election_id',
                                      help='The transaction id of the election you wish to query.')

    # parsers for showing/exporting config values
    subparsers.add_parser('show-config',
                          help='Show the current configuration')

    # parser for database-level commands
    subparsers.add_parser('init',
                          help='Init the database')

    subparsers.add_parser('drop',
                          help='Drop the database')

    # parser for starting BigchainDB
    start_parser = subparsers.add_parser('start',
                                         help='Start BigchainDB')

    start_parser.add_argument('--no-init',
                              dest='skip_initialize_database',
                              default=False,
                              action='store_true',
                              help='Skip database initialization')

    return parser


def main():
    utils.start(create_parser(), sys.argv[1:], globals())
