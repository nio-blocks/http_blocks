import requests
import re
from datetime import datetime
from urllib.request import quote
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty
from nio.metadata.properties.list import ListProperty
from nio.metadata.properties.object import ObjectProperty
from nio.metadata.properties.int import IntProperty
from nio.modules.scheduler.imports import Job
from nio.modules.threading.imports import Lock


class RESTPolling(Block):
    
    """ A base class for blocks that poll restful web services.

    """
    polling_interval = TimeDeltaProperty()
    retry_interval = TimeDeltaProperty()
    queries = ListProperty(str)
    retry_limit = IntProperty(default=1)

    def __init__(self):
        super().__init__()
        self._n_queries = 1
        self._url = None
        self._paging_url = None
        self._idx = 0
        self._poll_job = None
        self._retry_job = None
        self._init_retry_interval = None
        self._etags = [None]
        self._modifieds = [None]
        self._freshest = [None]
        self._prev_freshest = [None]
        self._prev_stalest = [None]
        self._curr_fresh = None
        self._curr_stale = None
        self._poll_lock = Lock()
        self._retry_count = 0

    def configure(self, context):
        super().configure(context)
        self._authenticate()
        self._init_retry_interval = self.retry_interval
        self._n_queries = len(self.queries)
        self._etags *= self._n_queries
        self._modifieds *= self._n_queries
        self._prev_freshest *= self._n_queries
        self._prev_stalest *= self._n_queries

    def start(self):
        super().start()
        self._poll_job = Job(
            self.poll,
            self.polling_interval,
            True
        )

    def stop(self):
        super().stop()
        if self._poll_job is not None:
            self._poll_job.cancel()

    def poll(self, paging=False):
        """ Called from user-defined block. Assumes that self.url contains
        the fully-formed endpoint intended for polling.

        Signals are notified from here.

        Args:
            paging (bool): Are we paging?

        Returns:
            None

        """
        self._poll_lock.acquire()
        headers = self._prepare_url(paging)
        url = self.paging_url or self.url
            
        self._logger.debug("%s: %s" %
                           ("Paging" if paging else "Polling", url))
        
        # Requests won't generally throw exceptions, but this provides a
        # bit of convenience for the block developer.
        try:
            resp = requests.get(url, headers=headers)
        except Exception as e:
            self._logger.error("GET request failed, details: %s" % e)
        
            # terminate the polling thread. this exception probably
            # indicates incorrect code in the user-defined block.
            return
            
        status = resp.status_code
        self.etag = self.etag if paging \
                     else resp.headers.get('ETag')
        self.modified = self.modified if paging \
                         else resp.headers.get('Last-Modified')

        if status != 200 and status != 304:
            self._logger.error(
                "Polling request returned status %d" % status
            )

            self._logger.debug("Attempting to re-authenticate.")
            self._authenticate()
            self._poll_lock.release()
            self._retry_poll(paging)
        else:
            
            # cancel the retry job if we were in a retry cycle
            self._retry_job = None
            self.retry_interval = self._init_retry_interval
            self._retry_count = 0

            # process the Response object and initiate paging if necessary
            try:
                signals, paging = self._process_response(resp)
                if signals:
                    self.notify_signals(signals)

                self._poll_lock.release()
                if paging:
                    self._paging()
                else:
                    self._poll_job = self._poll_job or Job(
                        self.poll,
                        self.polling_interval,
                        True
                    )
                    self._idx = (self._idx + 1) % self._n_queries
                    self._logger.debug(
                        "Preparing to query for: %s" % self.current_query)
                    

            except Exception as e:
                self._logger.exception(e)
                self._logger.error(
                    "Error while processing polling response: %s" % e
                )

        if self._poll_lock.locked():
            self._poll_lock.release()
                
    def _authenticate(self):
        """ This should be overridden in user-defined blocks.

        This is where an oauth handshake would take place or a url would
        be enriched with auth data.

        """
        pass

    def _prepare_url(self, paging):
        """ This should be overridden in user-defined blocks.

        Makes any necessary amendments, interpolations, etc. to self._url.

        """
        pass

    def _process_response(self, resp):
        """ This should be overridden in user-defined blocks.

        Do what thou wilt with the polling response.

        Args:
            resp (Response): A Response object (from requests lib)

        Returns:
            signals (list(Signal)): A list of signal object to notify.
            paging (dict/list/obj): Paging data, possibly None, from the
                recorded response.

        """
        pass

    def _paging(self):
        """ This can be overridden in user-defined blocks.

        """
        # cancel the polling job while we are paging
        if self._poll_job is not None:
            self._poll_job.cancel()
            self._poll_job = None

        self.poll(True)

    def _update_retry_interval(self):
        """ This should be overridden in user-defined blocks.

        Implement your retry strategy here. Exponential backoff? War?

        """
        pass

    def _retry_poll(self, paging=False):
        """ Helper method to schedule polling retries.

        """
        if self._poll_job is not None:
            self._poll_job.cancel()
            self._poll_job = None
        if self._retry_count < self.retry_limit:
            self._logger.debug("Retrying the polling job...")
            self._retry_count += 1
            self._retry_job = Job(
                self.poll,
                self.retry_interval,
                False,
                paging=paging
            )
            self._update_retry_interval()

    def update_freshness(self, posts):
        """ Bookkeeping for the state of the current query's polling.

        """
        self._curr_fresh = self.created_epoch(posts[0])
        self._curr_stale = self.created_epoch(posts[-1])
        if self._poll_job is not None:
            if self.prev_freshest is None or \
                   self.freshest > self.prev_freshest:
                self.prev_freshest = self.freshest
            self.freshest = self._curr_fresh

    def find_fresh_posts(self, posts):
        """ This can be overridden in user-defined blocks, if desired.

        Returns only those posts which were created after the newest
        post from the previous round of polling on the current query
        string.

        Note that the self.created_epoch expects dictionaries.
        Reimplement that method if you have another structure for posts.

        Args:
            posts (list(dict)): A list of posts.
        
        Returns:
            posts (list(dict)): The amended list of posts.

        """
        posts = [p for p in posts \
                 if self.created_epoch(p) > self.prev_freshest]
        return posts

    def created_epoch(self, post):
        """ Helper function to return the seconds since the epoch
        for the given post's 'created_time.

        Args:
            post (dict): Should contain a 'created_time' key.
        
        Returns:
            seconds (int): post[created_time] in seconds since epoch.
        
        """
        dt = self._parse_date(post.get(self._created_field, ''))
        return self._unix_time(dt)

    def _parse_date(self, date):
        """ Parses the service's date string format into a native datetime.

        This should be overridden in user-defined blocks.

        """
        exp = r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})"
        m = re.match(exp, date)
        return datetime(*[int(n) for n in m.groups(0)])

    def _unix_time(self, dt):
        epoch = datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return int(delta.total_seconds())
        
    @property
    def current_query(self):
        return quote(self.queries[self._idx])

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url

    @property
    def paging_url(self):
        return self._paging_url

    @paging_url.setter
    def paging_url(self, url):
        self._paging_url = url

    @property
    def etag(self):
        return self._etags[self._idx]

    @etag.setter
    def etag(self, etag):
        self._etags[self._idx] = etag

    @property
    def modified(self):
        return self._modifieds[self._idx]

    @modified.setter
    def modified(self, modified):
        self._modifieds[self._idx] = modified

    @property
    def freshest(self):
        return self._freshest[self._idx]
        
    @freshest.setter
    def freshest(self, timestamp):
        self._freshest[self._idx] = timestamp

    @property
    def prev_freshest(self):
        return self._prev_freshest[self._idx]
        
    @prev_freshest.setter
    def prev_freshest(self, timestamp):
        self._prev_freshest[self._idx] = timestamp

    @property
    def prev_stalest(self):
        return self._prev_stalest[self._idx]
        
    @prev_stalest.setter
    def prev_stalest(self, timestamp):
        self._prev_stalest[self._idx] = timestamp

