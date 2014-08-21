import itertools
import uuid
from inbox.util.addr import canonicalize_address as canonicalize
from inbox.models import Contact, MessageContactAssociation


def update_contacts_from_message(db_session, message, account_id):
    with db_session.no_autoflush:
        # First create Contact objects for any email addresses that we haven't
        # seen yet. We want to dedupe by canonicalized address, so this part is
        # a bit finicky.
        canonicalized_addresses = []
        address_iterator = itertools.chain(message.from_addr, message.to_addr,
                                           message.cc_addr, message.bcc_addr)
        canonicalized_addresses = [canonicalize(addr) for _, addr in
                                   address_iterator]

        existing_contacts = db_session.query(Contact).filter(
            Contact._canonicalized_address.in_(canonicalized_addresses),
            Contact.account_id == account_id).all()

        contact_map = {c._canonicalized_address: c for c in existing_contacts}
        for name, email_address in address_iterator:
            canonicalized_address = canonicalize(email_address)
            if canonicalized_address not in contact_map:
                new_contact = Contact(name=name, email_address=email_address,
                                      account_id=account_id, source='local',
                                      provider_name='inbox',
                                      uid=uuid.uuid4().hex)
                contact_map[canonicalized_address] = new_contact

        # Now associate each contact to the message.
        for field_name in ('from_addr', 'to_addr', 'cc_addr', 'bcc_addr'):
            field = getattr(message, field_name)
            for name, email_address in field:
                canonicalized_address = canonicalize(email_address)
                contact = contact_map.get(canonicalized_address)
                message.contacts.append(MessageContactAssociation(
                    contact=contact, field=field))
