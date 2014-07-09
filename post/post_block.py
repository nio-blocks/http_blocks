import json
from nio.common.block.base import Block
from nio.common.signal.base import Signal
from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.versioning.dependency import DependsOn
from nio.metadata.properties.string import StringProperty
from nio.metadata.properties.int import IntProperty
from nio.modules.web.imports import WebEngine, RESTHandler


class BuildSignal(RESTHandler):
    def __init__(self, notifier, logger):
        super().__init__('/')
        self.notify = notifier
        self._logger = logger

    def on_post(self, identifier, body, params):
        if isinstance(body, list) or isinstance(body, dict):
            body = body if isinstance(body, list) else [body]
            signals = [Signal(s) for s in body]
            self.notify(signals)
        else:
            self._logger.error("Invalid JSON in PostSignal request body")


@DependsOn("nio.modules.web", "1.0.0")
@Discoverable(DiscoverableType.block)
class PostSignal(Block):
    
    host = StringProperty(name='Host', default='127.0.0.1')
    port = IntProperty(name='Port', default=8182)
    endpoint = StringProperty(name='Endpoint', default='')

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

