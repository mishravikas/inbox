from sqlalchemy import desc
from sqlalchemy import Column, Integer, String, ForeignKey, Index, Enum
from sqlalchemy.orm import relationship

from inbox.models.base import MailSyncBase
from inbox.models.mixins import HasPublicID
from inbox.models.namespace import Namespace
from inbox.sqlalchemy_ext.util import BigJSON


class HasRevisions(object):
    """Mixin to signal that records in this table should be versioned in the
    transaction log."""
    def should_create_revision(self):
        """Subclasses can override this to signal that no revision should be
        created for a particular instance (for example, we don't want to
        version Part instances that aren't actual attachments)."""
        return True


class Transaction(MailSyncBase, HasPublicID):
    """ Transactional log to enable client syncing. """
    # Do delete transactions if their associated namespace is deleted.
    namespace_id = Column(Integer,
                          ForeignKey(Namespace.id, ondelete='CASCADE'),
                          nullable=False)
    namespace = relationship(
        Namespace,
        primaryjoin='and_(Transaction.namespace_id == Namespace.id, '
                    'Namespace.deleted_at.is_(None))')

    table_name = Column(String(20), nullable=False, index=True)
    record_id = Column(Integer, nullable=False, index=True)
    command = Column(Enum('insert', 'update', 'delete'), nullable=False)
    # The API representation of the object at the time the transaction is
    # generated.
    snapshot = Column(BigJSON, nullable=True)


Index('namespace_id_deleted_at', Transaction.namespace_id,
      Transaction.deleted_at)
Index('table_name_record_id', Transaction.table_name, Transaction.record_id)


def dict_delta(current_dict, previous_dict):
    """Return a dictionary consisting of the key-value pairs in
    current_dict that differ from those in previous_dict."""
    return {k: v for k, v in current_dict.iteritems() if k not in previous_dict
            or previous_dict[k] != v}


class RevisionMaker(object):
    def __init__(self, namespace=None):
        from inbox.api.kellogs import encode
        if namespace is not None:
            self.namespace_id = namespace.id
        else:
            self.namespace_id = None
        if namespace is not None:
            self.encoder_fn = lambda obj: encode(obj, namespace.public_id)
        else:
            self.encoder_fn = lambda obj: encode(obj)

    def create_revisions(self, session):
        for obj in session.new:
            # STOPSHIP(emfree): technically we could have deleted_at objects
            # here
            self.create_insert_revision(obj, session)
        for obj in session.dirty:
            if obj.deleted_at is not None:
                self.create_delete_revision(obj, session)
            else:
                self.create_update_revision(obj, session)
        for obj in session.deleted:
            self.create_delete_revision(obj, session)

    def should_create_revision(self, obj):
        if isinstance(obj, HasRevisions) and obj.should_create_revision():
            return True
        return False

    def create_insert_revision(self, obj, session):
        if not self.should_create_revision(obj):
            return
        snapshot = self.encoder_fn(obj)
        namespace_id = self.namespace_id or obj.namespace.id
        revision = Transaction(command='insert', record_id=obj.id,
                               table_name=obj.__tablename__, snapshot=snapshot,
                               namespace_id=namespace_id)
        session.add(revision)

    def create_delete_revision(self, obj, session):
        if not self.should_create_revision(obj):
            return
        # NOTE: The application layer needs to deal with purging all history
        # related to the object at some point.
        namespace_id = self.namespace_id or obj.namespace.id
        revision = Transaction(command='delete', record_id=obj.id,
                               table_name=obj.__tablename__,
                               namespace_id=namespace_id)
        session.add(revision)

    def create_update_revision(self, obj, session):
        if not self.should_create_revision(obj):
            return
        prev_revision = session.query(Transaction). \
            filter(Transaction.table_name == obj.__tablename__,
                   Transaction.record_id == obj.id). \
            order_by(desc(Transaction.id)).first()
        snapshot = self.encoder_fn(obj)
        if prev_revision is not None:
            if not dict_delta(snapshot, prev_revision.snapshot):
                return
        namespace_id = self.namespace_id or obj.namespace.id
        revision = Transaction(command='update', record_id=obj.id,
                               table_name=obj.__tablename__,
                               snapshot=snapshot,
                               namespace_id=namespace_id)
        session.add(revision)
