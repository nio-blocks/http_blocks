import requests
from nio.common.block.base import Block
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties.timedelta import TimeDeltaProperty
from nio.metadata.properties.holder import PropertyHolder
from nio.metadata.properties.string import StringProperty
from nio.metadata.properties.object import ObjectProperty
from nio.modules.scheduler.imports import Job


class OAuthCreds(PropertyHolder):
    """ Property holder for Twitter OAuth credentials.

    """
    consumer_key = StringProperty(default=None)
    app_secret = StringProperty(default=None)
    oauth_token = StringProperty(default=None)
    oauth_token_secret = StringProperty(default=None)

@Discoverable(DiscoverableType.block)
class RESTPolling(Block):
    
    """ A base class for blocks that poll restful web services.

    """
    polling_interval = TimeDeltaProperty()
    retry_interval = TimeDeltaProperty()
    creds = ObjectProperty(OAuthCreds)

    def __init__(self):
        super().__init__()
        self._url = None
        self._paging_field = None
        self._auth = None
        self._poll_job = None
        self._retry_job = None
        self._init_retry_interval = None

    def configure(self, context):
        super().configure(context)
        self._authenticate()
        self._init_retry_interval = self.retry_interval

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
        self._prepare_url(paging)
        headers = {"Content-type": "application/json"}
        self._logger.debug("%s: %s" %
                           ("Paging" if paging else "Polling", self._url))

        resp = requests.get(self._url, headers=headers)
        status = resp.status_code

        if resp.status_code != 200:
            self._logger.error(
                "Polling request returned status %d" % status
            )

            self._logger.debug("Attempting to re-authenticate.")
            self._authenticate()
            self._retry_poll(paging)
            self._update_retry_interval()
        else:
            
            # cancel the retry job if we were in a retry cycle
            self._retry_job = None
            self.retry_interval = self._init_retry_interval

            # process the Response object and initiate paging if necessary
            try:
                signals, paging = self._process_response(resp)

                if signals:
                    self.notify_signals(signals)

                if paging:
                    self._paging()
                else:
                    self._poll_job = self._poll_job or Job(
                        self.poll,
                        self.polling_interval,
                        True
                    )
            except Exception as e:
                self._logger.error(
                    "Error while processing polling response: %s" % e
                )

            
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
        if self._poll_job is not None:
            self._poll_job.cancel()
            self._poll_job = None
        self._retry_job = Job(
            self.poll,
            self.retry_interval,
            False,
            paging=paging
        )
