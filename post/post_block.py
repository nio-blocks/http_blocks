import json
from nio.common.block.base import Block
from nio.common.signal.base import Signal
from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.versioning.dependency import DependsOn
from nio.metadata.properties.string import StringProperty
from nio.metadata.properties.int import IntProperty
from nio.modules.web import WebEngine, RESTHandler


class BuildSignal(RESTHandler):
    def __init__(self, notifier, logger):
        super().__init__('/')
        self.notify = notifier
        self._logger = logger

    def on_post(self, req, rsp):
        body = req.get_body()
        if isinstance(body, dict):
            body = [body]
        elif isinstance(body, list):
            pass
        else:
            self._logger.error("Invalid JSON in PostSignal request body")
            return
        
        signals = [Signal(s) for s in body]
        self.notify(signals)


@DependsOn("nio.modules.web", "1.0.0")
@Discoverable(DiscoverableType.block)
class PostSignal(Block):
    
    host = StringProperty(title='Host', default='127.0.0.1')
    port = IntProperty(title='Port', default=8182)
    endpoint = StringProperty(title='Endpoint', default='')

    def __init__(self):
        super().__init__()
        self._server = None
        self._signals = []

    def configure(self, context):
        super().configure(context)
        self._server = WebEngine.create(self.endpoint, 
                                        {'socket_host': self.host,
                                         'socket_port': self.port})
        self._server.add_handler(BuildSignal(self.notify_signals,
                                             self._logger))
        context.hooks.attach('after_blocks_start', WebEngine.start)
