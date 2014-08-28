import sys
import time
from contextlib import contextmanager

from sqlalchemy import event
from sqlalchemy.orm.session import Session
from sqlalchemy.orm.interfaces import MapperOption
from sqlalchemy.orm.exc import NoResultFound

import sqlalchemy.orm.query

from inbox.config import config
from inbox.ignition import main_engine
from inbox.log import get_logger
log = get_logger()


class IgnoreSoftDeletesOption(MapperOption):
    """
    Automatically exclude soft-deleted objects from query results, including
    child objects on relationships.

    Based on:

        https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/GlobalFilter
    """
    propagate_to_loaders = True

    def process_query_conditionally(self, query):
        """process query during a lazyload"""
        query._params = query._params.union(dict(
            deleted_at=None,
        ))

    def process_query(self, query):
        """process query during a primary user query"""

        # apply bindparam values
        self.process_query_conditionally(query)

        parent_cls = query._mapper_zero().class_
        assert parent_cls is not None, "query against a single mapper required"
        filter_crit = parent_cls.deleted_at.is_(None)

        if query._criterion is None:
            query._criterion = filter_crit
        else:
            query._criterion &= filter_crit


class InboxQuery(sqlalchemy.orm.query.Query):

    def delete(self, *args):
        """ Not supported because we'd have to use internal APIs. """
        raise Exception("Not supported, use `session.delete()` instead!")

    def get(self, ident):
        """Can't use regular `.get()` on a query w/options already applied.

        Note that our semantics here are different from that of a regular
        query, in that we do not fetch directly from the session identity map.
        """
        cls = self._mapper_zero().class_
        try:
            return self.filter(cls.id == ident).one()
        except NoResultFound:
            return None


class InboxSession(object):
    """ Inbox custom ORM (with SQLAlchemy compatible API).

    Parameters
    ----------
    engine : <sqlalchemy.engine.Engine>
        A configured database engine to use for this session
    versioned : bool
        Do you want to enable the transaction log?
    ignore_soft_deletes : bool
        Whether or not to ignore soft-deleted objects in query results.
    namespace : *expunged* Namespace instance, optional
        If given, assume that all objects being operated on are local to that
        namespace when constructing revisions. Not for general use.
    """
    def __init__(self, engine, versioned=True, ignore_soft_deletes=True,
                 namespace=None):
        # TODO: support limiting on namespaces
        assert engine, "Must set the database engine"

        args = dict(bind=engine, autoflush=True, autocommit=False)
        self.ignore_soft_deletes = ignore_soft_deletes
        if ignore_soft_deletes:
            args['query_cls'] = InboxQuery
        self._session = Session(**args)

        if versioned:
            from inbox.models.transaction import RevisionMaker
            self.revision_maker = RevisionMaker(namespace)

            @event.listens_for(self._session, 'after_flush')
            def after_flush(session, flush_context):
                """
                Hook to log revision snapshots. Must be post-flush in order to
                grab object IDs on new objects.
                """
                self.revision_maker.create_revisions(session)

    def query(self, *args, **kwargs):
        q = self._session.query(*args, **kwargs)
        if self.ignore_soft_deletes:
            return q.options(IgnoreSoftDeletesOption())
        else:
            return q

    def add(self, instance):
        if not self.ignore_soft_deletes or not instance.is_deleted:
            self._session.add(instance)
        else:
            raise Exception("Why are you adding a deleted object?")

    def add_all(self, instances):
        if True not in [i.is_deleted for i in instances] or \
                not self.ignore_soft_deletes:
            self._session.add_all(instances)
        else:
            raise Exception("Why are you adding a deleted object?")

    def delete(self, instance):
        if self.ignore_soft_deletes:
            instance.mark_deleted()
            # just to make sure
            self._session.add(instance)
        else:
            self._session.delete(instance)

    def begin(self):
        self._session.begin()

    def commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()

    def flush(self):
        self._session.flush()

    def close(self):
        self._session.close()

    def expunge(self, obj):
        self._session.expunge(obj)

    @property
    def no_autoflush(self):
        return self._session.no_autoflush


cached_engine = None


@contextmanager
def session_scope(versioned=True, ignore_soft_deletes=True, namespace=None):
    """ Provide a transactional scope around a series of operations.

    Takes care of rolling back failed transactions and closing the session
    when it goes out of scope.

    Note that sqlalchemy automatically starts a new database transaction when
    the session is created, and restarts a new transaction after every commit()
    on the session. Your database backend's transaction semantics are important
    here when reasoning about concurrency.

    Parameters
    ----------
    versioned : bool
        Do you want to enable the transaction log?
    ignore_soft_deletes : bool
        Whether or not to ignore soft-deleted objects in query results.
    namespace : *expunged* Namespace instance, optional
        If given, assume that all objects being operated on are local to that
        namespace when constructing revisions. Not for general use.

    Yields
    ------
    InboxSession
        The created session.
    """

    global cached_engine
    if cached_engine is None:
        cached_engine = main_engine()
        log.info("Don't yet have engine... creating default from ignition",
                 engine=id(cached_engine))

    session = InboxSession(cached_engine,
                           versioned=versioned,
                           ignore_soft_deletes=ignore_soft_deletes,
                           namespace=namespace)
    try:
        if config.get('LOG_DB_SESSIONS'):
            start_time = time.time()
            calling_frame = sys._getframe().f_back.f_back
            call_loc = '{}:{}'.format(calling_frame.f_globals.get('__name__'),
                                      calling_frame.f_lineno)
            logger = log.bind(engine_id=id(cached_engine),
                              session_id=id(session), call_loc=call_loc)
            logger.info('creating db_session',
                        sessions_used=cached_engine.pool.checkedout())
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        if config.get('LOG_DB_SESSIONS'):
            lifetime = time.time() - start_time
            logger.info('closing db_session', lifetime=lifetime,
                        sessions_used=cached_engine.pool.checkedout())
        session.close()
