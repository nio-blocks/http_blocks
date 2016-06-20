import requests
import re
from datetime import datetime
from urllib.request import quote, unquote
from nio.block.base import Block
from nio.properties.timedelta import TimeDeltaProperty
from nio.properties.list import ListProperty
from nio.properties.int import IntProperty
from nio.properties.string import StringProperty
from nio.modules.scheduler import Job
from threading import Lock
from nio.util.threading.spawn import spawn
from nio.signal.status import BlockStatusSignal
from nio.util.runner import RunnerStatus
from nio.types import StringType


class RESTPolling(Block):

    """ A base class for blocks that poll restful web services.

    """
    polling_interval = TimeDeltaProperty(title='Polling Interval',
                                         default={"seconds": 20})
    retry_interval = TimeDeltaProperty(title='Retry Interval',
                                       default={"seconds": 60})
    queries = ListProperty(StringType, title='Query Strings', default=[])
    include_query = StringProperty(title='Include Query Field',
                                   allow_none=True)
    retry_limit = IntProperty(title='Retry Limit', default=3)

    def __init__(self):
        super().__init__()
        self._n_queries = 0
        self._url = None
        self._paging_url = None
        self._page_num = 1
        self._idx = 0
        self._poll_job = None
        self._retry_job = None
        self._retry_interval = None
        self._etags = [None]
        self._modifieds = [None]
        self._freshest = [None]
        self._prev_freshest = [None]
        self._prev_stalest = [None]
        self._curr_fresh = None
        self._curr_stale = None
        self._poll_lock = Lock()
        self._retry_count = 0
        self._auth = None
        self._recent_posts = None
        self._num_locks = 0
        self._max_locks = 5  # the max number of lock acquirers that can wait

        # this should be overridden in child blocks to refer to the actual
        # "created at" field for items returned from the particular service
        self._created_field = 'created_at'

    def configure(self, context):
        super().configure(context)
        self._authenticate()
        self._retry_interval = self.retry_interval()
        self._n_queries = len(self.queries())
        self._etags *= self._n_queries
        self._modifieds *= self._n_queries
        self._prev_freshest *= self._n_queries
        self._prev_stalest *= self._n_queries
        self._recent_posts = [None] * self._n_queries

    def start(self):
        super().start()
        if self.polling_interval().total_seconds() > 0:
            self._poll_job = Job(
                self.poll,
                self.polling_interval(),
                True
            )
            spawn(self.poll)
        else:
            self.logger.info("No poll job")

    def stop(self):
        super().stop()
        if self._poll_job is not None:
            self._poll_job.cancel()
        if self._retry_job is not None:
            self._retry_job.cancel()

    def process_signals(self, signals):
        if self._retry_job is None:
            for signal in signals:
                self.poll()
        else:
            self.logger.debug(
                "A 'retry' is currently scheduled. "
                "Ignoring incoming signals."
            )


    def poll(self, paging=False, in_retry=False):
        """ Called from user-defined block. Assumes that self.url contains
        the fully-formed endpoint intended for polling.

        Signals are notified from here.

        Args:
            paging (bool): Are we paging?
            in_retry (bool): was poll called form a retry_job

        Returns:
            None

        """
        if self._n_queries == 0:
            return

        if self._num_locks >= self._max_locks:
            self.logger.warning(
                "Currently {} locks waiting to be acquired. This is more than "
                "the max of {}. Ignoring poll".format(
                    self._num_locks, self._max_locks))
            return

        # Increment the number of lock waiters so we don't build up too many
        self._num_locks += 1
        with self._poll_lock:
            if self._retry_job is None or in_retry:
                self._locked_poll(paging)
            else:
                self.logger.debug(
                    "A 'retry' is already scheduled. "
                    "Skipping this poll."
                )

        self._num_locks -= 1

    def _locked_poll(self, paging=False):
        """ Execute the poll, while being assured that resources are locked """

        if not paging:
            # This is the first page of a new query.
            self._recent_posts[self._idx] = {}
            self.page_num = 1

        headers = self._prepare_url(paging)
        url = self.paging_url or self.url

        self.logger.debug(
            "{}: {}".format("Paging" if paging else "Polling", url)
        )

        resp = self._execute_request(url, headers, paging)
        if resp is None:
            return

        self.etag = self.etag if paging else resp.headers.get('ETag')
        self.modified = self.modified if paging \
            else resp.headers.get('Last-Modified')

        try:
            if not self._validate_response(resp):
                self._on_failure(resp, paging, url)
            else:
                self._on_success(resp, paging)
        except Exception as e:
            self.logger.exception(e)
            self.logger.warning(
                "Error processing polling response: {}: {}".format(
                    type(e).__name__, str(e))
            )

    def _on_failure(self, resp, paging, url):
        """ This can be overridden in user-defined blocks.

        Defines how failed polling requests will be handled.

        """
        try:
            status_code = resp.status_code
            resp = resp.json()
        except:
            # Response is not json.
            # This is fine. We're just logging a warning about the resp.
            pass
        finally:
            self.logger.warning(
                "Polling request of {} returned status {}: {}".format(
                    url, status_code, resp)
            )
            self._retry(paging)

    def _on_success(self, resp, paging):
        """ This can be overridden in user-defined blocks.

        Defines how successful polling requests will be handled.

        """
        self._reset_retry_cycle()

        signals, paging = self._process_response(resp)
        signals = self._discard_duplicate_posts(signals)

        # add the include_query attribute if it is configured
        if self.include_query() and signals is not None:
            for s in signals:
                setattr(
                    s, self.include_query(), unquote(self.current_query)
                )

        if signals:
            self.notify_signals(signals)

        if paging:
            self.page_num += 1
            self._paging()
        else:
            self._epilogue()

    def _reset_retry_cycle(self):
        """ This can be overridden in user-defined blocks.

        Logic for cleaning up retry jobs and counters goes here.

        """
        # cancel the retry job if we were in a retry cycle
        if self._retry_job is not None:
            self._retry_job.cancel()
            self._retry_job = None
        self._retry_interval = self.retry_interval()
        # this poll was a success so reset the retry count
        self._retry_count = 0

    def _epilogue(self):
        """ This can be overridden in user-defined blocks.

        Defines behavior after a query has been fully processed,
        when we are ready for the next query. That is, when paging
        is done and retries are cleared.

        """
        if self.polling_interval().total_seconds() > 0:
            self._poll_job = self._poll_job or Job(
                self.poll,
                self.polling_interval(),
                True
            )
        self._increment_idx()
        if self.queries():
            self.logger.debug(
                "Preparing to query for: %s" % self.current_query
            )

    def _authenticate(self):
        """ This should be overridden in user-defined blocks.

        This is where an oauth handshake would take place or a url would
        be enriched with auth data.

        """
        pass

    def _validate_response(self, resp):
        """ This can be overridden in user-defined blocks.

        This is where we determine if a response is bad and we need a retry.

        Returns:
            validation (bool): True if response is good, False if bad.

        """
        return resp.status_code == 200 or resp.status_code == 304

    def _retry(self, paging):
        """

        This is where we determine what to do on a bad poll response.

        """
        self.logger.debug("Attempting to re-authenticate.")
        self._authenticate()
        self.logger.debug("Attempting to retry poll.")
        self._retry_poll(paging)

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

        Logic for handling paging situations.

        """
        # cancel the polling job while we are paging
        if self._poll_job is not None:
            self._poll_job.cancel()
            self._poll_job = None

        self._locked_poll(True)

    def _update_retry_interval(self):
        """ This should be overridden in user-defined blocks.

        Implement your retry strategy here. Exponential backoff? War?

        """
        self.logger.debug("Updating retry interval from {} to {}".
                           format(self._retry_interval,
                                  self._retry_interval * 2))
        self._retry_interval *= 2

    def _retry_poll(self, paging=False):
        """ Helper method to schedule polling retries.

        """
        if self._poll_job is not None:
            self._poll_job.cancel()
            self._poll_job = None
        if self._retry_count < self.retry_limit():
            self.logger.debug("Retrying the polling job...")
            self._retry_count += 1
            self._retry_job = Job(
                self.poll,
                self._retry_interval,
                False,
                paging=paging,
                in_retry=True
            )
            self._update_retry_interval()
        else:
            self.logger.error("Out of retries. "
                               "Aborting and changing status to Error.")
            status_signal = BlockStatusSignal(
                BlockStatus.error, 'Out of retries.')

            # Leaving source for backwards compatibility
            # In the future, you will know that a status signal is a block
            # status signal when it contains service_name and name
            #
            # TODO: Remove when source gets added to status signals in nio
            setattr(status_signal, 'source', 'Block')

            self.notify_management_signal(status_signal)

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
        posts = [p for p in posts
                 if self.created_epoch(p) > (self.prev_freshest or 0)]
        return posts

    def _discard_duplicate_posts(self, posts):
        """ Removes sigs that were already found by another query.

        Each query acts independently so if a post matches multiple
        queries, then it will be notified for each one. This method
        keeps track of the all the most recent posts for each query
        and discards posts if they are already here.

        Args:
            posts (list(dict)): A list of posts.
            first_page (bool): True if this is the first page of query.

        Returns:
            posts (list(dict)): The amended list of posts.

        """
        # No need to try to discards posts if there is only one query.
        if self._n_queries <= 1:
            return posts

        # Return only posts that are not in self._recent_posts.
        result = []
        for post in posts:
            post_id = self._get_post_id(post)
            is_dupe = False
            valid_records = [r for r in self._recent_posts if r is not None]
            for record in valid_records:
                if post_id in record:
                    is_dupe = True
                    break

            if not post_id or not is_dupe:
                result.append(post)
                self._recent_posts[self._idx][post_id] = True

        return result

    def _get_post_id(self, post):
        """ Returns a uniquely identifying string for a post.

        This should be overridden in user-defined blocks.

        Args:
            post (dict): A post.
        Returns:
            id (string): A string that uniquely identifies a
                         post. None indicated that the post should
                         be treated as unique.
        """
        return None

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

    def _execute_request(self, url, headers, paging):
        """ Execute the request, accounting for possible errors

        """
        # Requests won't generally throw exceptions, but this provides a
        # bit of convenience for the block developer.
        resp = None
        try:
            if self._auth is not None:
                resp = requests.get(url, headers=headers, auth=self._auth)
            else:
                resp = requests.get(url, headers=headers)
        except Exception as e:
            self.logger.warning("GET request failed, details: %s" % e)

            # Use the usual retry strategy to resolve the error
            self._retry(paging)
        finally:
            return resp

    def _unix_time(self, dt):
        epoch = datetime.utcfromtimestamp(0)
        delta = dt - epoch
        return int(delta.total_seconds())

    def _increment_idx(self):
        self._idx = (self._idx + 1) % self._n_queries

    @property
    def current_query(self):
        return quote(self.queries()[self._idx])

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
    def page_num(self):
        return self._page_num

    @page_num.setter
    def page_num(self, num):
        self._page_num = num

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
