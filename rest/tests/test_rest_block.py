from unittest.mock import patch
from requests import Response
from http_blocks.rest.rest_block import RESTPolling
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.modules.threading.imports import Event


class RESTBlock(RESTPolling):
    
    def __init__(self, event):
        super().__init__()
        self._poll_event = event
    
    def poll(self, paging=False):
        self._poll_event.set()
        super().poll(paging)

class RESTRetry(RESTPolling):
    def __init__(self, events):
        super().__init__()
        self._poll_events = events

    def poll(self, paging=False):
        self._poll_events[0].set()
        self._poll_events = self._poll_events[1:]
        super().poll(paging)

class TestRESTPolling(NIOBlockTestCase):
    
    @patch("http_blocks.rest.rest_block.RESTPolling.poll")
    @patch("http_blocks.rest.rest_block.RESTPolling._authenticate")
    def test_machinery(self, mock_auth, mock_poll):
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
        mock_poll.assert_called_once_with(False)
        mock_auth.assert_called_once()

        blk.stop()

    @patch("requests.get")
    @patch("http_blocks.rest.rest_block.RESTPolling._process_response")
    @patch("http_blocks.rest.rest_block.RESTPolling._prepare_url")
    def test_poll(self, mock_prep, mock_proc, mock_get):
        e = Event()
        blk = RESTBlock(e)
        mock_get.return_value = Response()
        mock_get.return_value.status_code = 200
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
        
        mock_prep.assert_called_once_with(False)
        mock_get.assert_called_once()
        mock_proc.assert_called_once()

        blk.stop()

    @patch("requests.get")
    @patch("http_blocks.rest.rest_block.RESTPolling._authenticate")
    @patch("http_blocks.rest.rest_block.RESTPolling._retry_poll")
    def test_sched_retry(self, mock_retry, mock_auth, mock_get):
        es = [Event(), Event()]
        blk = RESTRetry(es)
        mock_get.return_value = Response()
        mock_get.return_value.status_code = 400
        self.configure_block(blk, {
            "polling_interval": {
                "seconds": 1
            },
            "retry_interval": {
                "seconds": 1
            }
        })
        blk.start()
        es[1].wait(2)

        self.assertEqual(mock_auth.call_count, 2)
        self.assertEqual(mock_retry.call_count, 1)
        self.assertEqual(mock_get.call_count, 1)

        blk.stop()
        
