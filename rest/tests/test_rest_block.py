from ..rest_block import RESTPolling
from unittest.mock import patch, Mock
from requests import Response
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.modules.threading import Event
from nio.common.signal.base import Signal


class RESTBlock(RESTPolling):

    def __init__(self, event):
        super().__init__()
        self._poll_event = event

    def poll(self, paging=False):
        super().poll(paging)
        self._poll_event.set()


class MultiQueryREST(RESTPolling):
    def __init__(self, event):
        super().__init__()
        self._num_polls = 0
        self._poll_event = event

    def poll(self, paging=False):
        super().poll(paging)
        self._num_polls += 1
        if self._num_polls == 2:
            self._poll_event.set()

    def _get_post_id(self, signal):
        return signal._id

class RESTRetry(RESTPolling):
    def __init__(self, events):
        super().__init__()
        self._poll_events = events

    def poll(self, paging=False):
        self._poll_events[0].set()
        if len(self._poll_events) == 1:
            return
        self._poll_events = self._poll_events[1:]
        super().poll(paging)

class TestRESTPolling(NIOBlockTestCase):

    @patch.object(RESTPolling, "poll")
    @patch.object(RESTPolling, "_authenticate")
    def test_machinery(self, mock_auth, mock_poll):
        e = Event()
        blk = RESTBlock(e)
        self.configure_block(blk, {
            "polling_interval": {
                "seconds": 1
            },
            "retry_interval": {
                "seconds": 1
            },
            "queries": [
                "foobar"
            ]
        })
        blk.start()
        e.wait(2)
        mock_poll.assert_called_once_with(False)
        mock_auth.assert_called_once()

        blk.stop()

    @patch("requests.get")
    @patch.object(RESTPolling, "_process_response")
    @patch.object(RESTPolling, "_prepare_url")
    def test_poll(self, mock_prep, mock_proc, mock_get):
        e = Event()
        blk = RESTBlock(e)
        mock_get.return_value = Response()
        mock_get.return_value.status_code = 200
        mock_proc.return_value = [None, None]
        self.configure_block(blk, {
            "polling_interval": {
                "seconds": 1
            },
            "retry_interval": {
                "seconds": 1
            },
            "queries": [
                "foobar"
            ]
        })
        blk.start()
        e.wait(2)

        mock_prep.assert_called_once_with(False)
        mock_get.assert_called_once()
        mock_proc.assert_called_once()

        blk.stop()

    @patch("requests.get")
    @patch.object(RESTPolling, "_process_response")
    @patch.object(RESTPolling, "_prepare_url")
    def test_paging(self, mock_prep, mock_proc, mock_get):
        e = Event()
        blk = RESTBlock(e)
        mock_get.return_value = Response()
        mock_get.return_value.status_code = 200
        mock_proc.side_effect = [([None, None], True),
                                 ([None, None], True),
                                 ([None, None], False)]
        self.configure_block(blk, {
            "polling_interval": {
                "seconds": 1
            },
            "retry_interval": {
                "seconds": 1
            },
            "queries": [
                "foobar"
            ]
        })
        blk.start()
        e.wait(2)

        self.assertEqual(blk.page_num, 3)

        blk.stop()

    @patch("requests.get")
    @patch.object(RESTPolling, "_process_response")
    @patch.object(RESTPolling, "_prepare_url")
    def test_paging_multi_query(self, mock_prep, mock_proc, mock_get):
        e = Event()
        blk = MultiQueryREST(e)
        mock_get.return_value = Response()
        mock_get.return_value.status_code = 200

        mock_proc.side_effect = [
            ([
                Signal({'_id': 1}),
                Signal({'_id': 2})
            ], True),
            ([
                Signal({'_id': 3}),
                Signal({'_id': 4})
            ], True),
            ([
                Signal({'_id': 5}),
                Signal({'_id': 6})
            ], False),
            ([
                Signal({'_id': 7}),
                Signal({'_id': 8})
            ], True),
            ([
                Signal({'_id': 9}),
                Signal({'_id': 1})
            ], False)
        ]

        self.configure_block(blk, {
            "polling_interval": {
                "seconds": 0.5
            },
            "retry_interval": {
                "seconds": 1
            },
            "queries": [
                "foobar",
                "bazqux"
            ]
        })
        blk.start()
        e.wait(2)

        self.assert_num_signals_notified(9, blk)
        self.assertEqual(blk.page_num, 2)

        blk.stop()

    @patch("requests.get")
    @patch.object(RESTPolling, "_authenticate")
    @patch.object(RESTPolling, "_retry_poll")
    def test_sched_retry(self, mock_retry, mock_auth, mock_get):
        es = [Event(), Event()]
        blk = RESTRetry(es)
        mock_get.return_value = Mock()
        mock_get.return_value.status_code = 400
        mock_get.return_value.json.return_value = \
            {
                'meta': {
                    'error_message': 'you cannot view this resource',
                    'code': 400,
                    'error_type': 'APINotAllowedError'
                }
            }
        self.configure_block(blk, {
            "log_level": "WARNING",
            "polling_interval": {
                "seconds": 1
            },
            "retry_interval": {
                "seconds": 1
            },
            "queries": [
                "foobar"
            ]
        })
        blk.start()
        es[1].wait(2)

        self.assertEqual(mock_auth.call_count, 2)
        self.assertEqual(mock_retry.call_count, 1)
        self.assertEqual(mock_get.call_count, 1)

        blk.stop()

    @patch("requests.get", side_effect=Exception("mock get fail"))
    @patch.object(RESTPolling, "_authenticate")
    @patch.object(RESTPolling, "_retry_poll")
    def test_get_error_retry(self, mock_retry, mock_auth, mock_get):
        es = [Event(), Event()]
        blk = RESTRetry(es)
        self.configure_block(blk, {
            "log_level": "DEBUG",
            "polling_interval": {
                "seconds": 1
            },
            "retry_interval": {
                "seconds": 1
            },
            "queries": [
                "foobar"
            ]
        })
        blk.start()
        es[1].wait(2)

        self.assertEqual(mock_auth.call_count, 2)
        self.assertEqual(mock_retry.call_count, 1)
        self.assertEqual(mock_get.call_count, 1)

        blk.stop()

    @patch("requests.get")
    @patch.object(RESTPolling, "_process_response")
    @patch.object(RESTPolling, "_prepare_url")
    def test_no_dupes(self, mock_prep, mock_proc, mock_get):
        e = Event()
        blk = MultiQueryREST(e)
        mock_get.return_value = Response()
        mock_get.return_value.status_code = 200

        mock_proc.return_value = [
            Signal({'_id': 1}),
            Signal({'_id': 2})
        ], False

        self.configure_block(blk, {
            "polling_interval": {
                "seconds": 0.5
            },
            "retry_interval": {
                "seconds": 1
            },
            "queries": [
                "foobar",
                "bazqux"
            ]
        })
        blk.start()
        e.wait(2)

        self.assert_num_signals_notified(2, blk)
        blk.stop()

    @patch("requests.get")
    @patch.object(RESTPolling, "poll")
    @patch.object(RESTPolling, "_authenticate")
    def test_no_queries(self, mock_auth, mock_poll, mock_get):
        e = Event()
        blk = RESTBlock(e)
        self.configure_block(blk, {
            "polling_interval": {
                "seconds": 1
            },
            "retry_interval": {
                "seconds": 1
            }
        })
        blk.start()
        e.wait(2)
        blk.process_signals([Signal()])
        mock_auth.assert_called_once()
        self.assertEqual(mock_poll.call_count, 2)
        self.assertEqual(mock_get.call_count, 0)
        blk.stop()

    def test_resp_on_failure(self):
        blk = RESTPolling()
        blk._retry = Mock()
        r = Mock()
        r.json = Exception()
        self.assertFalse(blk._retry.called)
        blk._on_failure(r, paging=False, url='the_url')
        self.assertTrue(blk._retry.called)
