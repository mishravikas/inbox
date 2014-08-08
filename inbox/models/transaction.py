from sqlalchemy import event, desc
from sqlalchemy import Column, Integer, String, ForeignKey, Index, Enum
from sqlalchemy.orm import relationship

from inbox.log import get_logger
log = get_logger()

from inbox.models.base import MailSyncBase
from inbox.models.mixins import HasPublicID
from inbox.models.namespace import Namespace
from inbox.sqlalchemy_ext.util import BigJSON


def dict_delta(current_dict, previous_dict):
    """Return a dictionary consisting of the key-value pairs in
    current_dict that differ from those in previous_dict."""
    return {k: v for k, v in current_dict.iteritems() if k not in previous_dict
            or previous_dict[k] != v}


class HasRevisions(object):
    """Mixin that signals that records in this table should be versioned in the
    transaction log."""
    def should_record(self):
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


class RevisionMaker(object):
    def __init__(self, namespace=None):
        from inbox.api.kellogs import encode
        if namespace is not None:
            self.namespace_id = namespace.id
        else:
            self.namespace_id = None
        # STOPSHIP(emfree) figure out if we can make this work just through
        # judicious eager-loading.
        if namespace is not None:
            self.encoder_fn = lambda obj: encode(obj, namespace.public_id)
        else:
            self.encoder_fn = lambda obj: encode(obj)

    def create_insert_revision(self, obj, session):
        if not self._should_create_revision(obj):
            return
        snapshot = self.encoder_fn(obj)
        namespace_id = self.namespace_id or obj.namespace.id
        revision = Transaction(command='insert', record_id=obj.id,
                               table_name=obj.__tablename__, snapshot=snapshot,
                               namespace_id=namespace_id)
        session.add(revision)

    def create_delete_revision(self, obj, session):
        if not self._should_create_revision(obj):
            return
        # NOTE: The application layer needs to deal with purging all history
        # related to the object at some point.
        namespace_id = self.namespace_id or obj.namespace.id
        revision = Transaction(command='delete', record_id=obj.id,
                               table_name=obj.__tablename__,
                               namespace_id=namespace_id)
        session.add(revision)

    def create_update_revision(self, obj, session):
        if not self._should_create_revision(obj):
            return
        prev_revision = session.query(Transaction). \
            filter(Transaction.table_name == obj.__tablename__,
                   Transaction.record_id == obj.id). \
            order_by(desc(Transaction.id)).first()
        snapshot = self.encoder_fn(obj)
        delta = dict_delta(snapshot, prev_revision.snapshot)
        if delta:
            namespace_id = self.namespace_id or obj.namespace.id
            revision = Transaction(command='update', record_id=obj.id,
                                   table_name=obj.__tablename__,
                                   snapshot=snapshot,
                                   namespace_id=namespace_id)
            session.add(revision)

    def _should_create_revision(self, obj):
        if isinstance(obj, HasRevisions) and obj.should_record:
            return True
        return False


def versioned_session(session, revision_maker):
    @event.listens_for(session, 'after_flush')
    def after_flush(session, flush_context):
        """ Hook to log revision deltas. Must be post-flush in order to grab
            object IDs on new objects.
        """
        for obj in session.new:
            # STOPSHIP(emfree): technically we could have deleted_at objects
            # here
            revision_maker.create_insert_revision(obj, session)
        for obj in session.dirty:
            if obj.deleted_at is not None:
                revision_maker.create_delete_revision(obj, session)
            else:
                revision_maker.create_update_revision(obj, session)
        for obj in session.deleted:
            revision_maker.create_delete_revision(obj, session)
    return session
