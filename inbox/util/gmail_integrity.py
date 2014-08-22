import httplib2
from apiclient.discovery import build
from oauth2client.client import OAuth2Credentials
from inbox.log import configure_logging
configure_logging(False)
from inbox.models.session import session_scope
from inbox.models.backends.gmail import GmailAccount
from inbox.models.backends.imap import ImapThread
from inbox.auth.gmail import (OAUTH_CLIENT_ID,
                              OAUTH_CLIENT_SECRET,
                              OAUTH_ACCESS_TOKEN_URL)
OAUTH_ACCESS_TOKEN_URL = OAUTH_ACCESS_TOKEN_URL
SOURCE_APP_NAME = 'Testing the Gmail API'


class GmailThread(object):
    def __init__(self, hex_thrid, labels=None):
        self.hex_thrid = hex_thrid
        self.labels = labels or []

    def __repr__(self):
        return 'hex_thrid: {}, labels: {}'.format(self.hex_thrid, self.labels)


class Diff(object):
    def __init__(self, local, remote):
        self.local = local
        self.remote = remote

    def __repr__(self):
        return 'local: {}, remote: {}'.format(self.local, self.remote)


class GmailAPIClient(object):
    def __init__(self, account_id):
        self.account_id = account_id
        with session_scope() as db_session:
            account = db_session.query(GmailAccount).get(account_id)
            client_id = account.client_id or OAUTH_CLIENT_ID
            client_secret = account.client_secret or OAUTH_CLIENT_SECRET
            access_token = account.access_token
            refresh_token = account.refresh_token
            expiry = account.access_expiry

        credentials = OAuth2Credentials(access_token, client_id, client_secret,
                                        refresh_token, expiry,
                                        OAUTH_ACCESS_TOKEN_URL,
                                        SOURCE_APP_NAME)

        http = httplib2.Http()
        http = credentials.authorize(http)

        self.service = build(serviceName='gmail', version='v1', http=http)
        self._label_cache = {}

    def get_g_thrids(self):
        all_threads = []
        page_token = None
        while True:
            partial_results = self.service.users().threads(). \
                list(userId='me', pageToken=page_token).execute()
            threads = [GmailThread(item['id']) for item in
                       partial_results['threads']]
            all_threads.extend(threads)
            if 'nextPageToken' in partial_results:
                page_token = partial_results['nextPageToken']
            else:
                print 'fetched {} thrids'.format(len(all_threads))
                return all_threads

    def get_thread_labels(self, hex_thrid):
        labels = []
        thread = self.service.users().threads(). \
            get(userId='me', id=hex_thrid).execute()
        for msg in thread['messages']:
            label_ids = msg.get('labelIds') or []
            for id in label_ids:
                if id in self._label_cache:
                    label = self._label_cache[id]
                else:
                    label = self.service.users().labels(). \
                        get(userId='me', id=id).execute()['name']
                    self._label_cache[id] = label
                labels.append(label)
        return labels

    def get_all_labels(self):
        threads = self.get_g_thrids()
        for thread in threads:
            labels = self.get_thread_labels(thread.hex_thrid)
            thread.labels = labels
        return threads

    def diff_with_local(self):
        remote_threads = self.get_all_labels()
        diffs = {}
        with session_scope() as db_session:
            for remote_thread in remote_threads:
                remote_labels = {l.lower() for l in remote_thread.labels}
                if 'inbox' not in remote_labels:
                    remote_labels.add('archive')
                int_thrid = int(remote_thread.hex_thrid, 16)
                local_thread = db_session.query(ImapThread).filter(
                    ImapThread.namespace_id == self.account_id,
                    ImapThread.g_thrid == int_thrid).first()
                if local_thread is None:
                    diffs[remote_thread.hex_thrid] = Diff(None, remote_labels)
                else:
                    local_labels = {t.name.lower() for t in local_thread.tags}
                    # Don't get these from the Gmail API.
                    local_labels.discard('all')
                    local_labels.discard('important')
                    if local_labels != remote_labels:
                        diffs[remote_thread.hex_thrid] = Diff(local_labels,
                                                              remote_labels)
        return diffs
