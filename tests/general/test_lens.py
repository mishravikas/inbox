import pytest

from sqlalchemy import desc

from tests.util.base import config, api_client
config()

from inbox.models import Lens, Transaction

NAMESPACE_ID = 1


def test_lens_tx(api_client, db):
    api_client.post_data('/drafts/', {
        'subject': 'Calaveras Dome / Hammer Dome',
        'to': [{'name': 'Somebody', 'email': 'somebody@example.com'}],
        'cc': [{'name': 'Another Person', 'email': 'another@example.com'}]
    })

    transaction = db.session.query(Transaction). \
        filter(Transaction.table_name == 'message'). \
        order_by(desc(Transaction.id)).first()

    filter = Lens(subject='/Calaveras/')
    assert filter.match(transaction)

    filter = Lens(subject='Calaveras')
    assert not filter.match(transaction)

    filter = Lens(from_addr='inboxapptest@gmail.com')
    assert filter.match(transaction)

    filter = Lens(from_addr='/inboxapp/')
    assert filter.match(transaction)

    filter = Lens(cc_addr='/Another/')
    assert filter.match(transaction)

    filter = Lens(subject='/Calaveras/', any_email='Nobody')
    assert not filter.match(transaction)

    filter = Lens(subject='/Calaveras/', any_email='/inboxapp/')
    assert filter.match(transaction)

    with pytest.raises(ValueError):
        filter = Lens(subject='/*/')
