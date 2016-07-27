def test_init_transaction(b, user_vk):
    from bigchaindb.transaction import (
        Fulfillment,
        Condition,
        Transaction,
        TransactionType,
    )
    from bigchaindb.util import validate_fulfillments
    ffill = Fulfillment([user_vk])
    cond = Condition([user_vk])
    tx = Transaction([ffill], [cond], TransactionType.CREATE)
    import ipdb; ipdb.set_trace()
    tx = tx.to_dict()

    assert tx['transaction']['fulfillments'][0]['owners_before'][0] == b.me
    assert tx['transaction']['conditions'][0]['owners_after'][0] == user_vk
    assert validate_fulfillments(tx)
