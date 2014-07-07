from nio.common.block.base import Block
from nio.common.signal.base import Signal
from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.versioning.dependency import DependsOn
from nio.metadata.properties.string import StringProperty
from nio.metadata.properties.int import IntProperty
from nio.modules.web.imports import WebEngine, RESTHandler


class BuildSignal(RESTHandler):
    def __init__(self, notifier):
        self.notify = notifier
        self.signals = []

    def on_post(self, identifier, body, params):
        raw_signals = json.loads(body)
        if not isinstance(raw_signals, list):
            raw_signals = [raw_signals]

        self.signals = [Signal(s) for s in raw_signals]
        self.notify(self.signals)
        self.signals = []


@DependsOn("nio.modules.web", "1.0.0")
@Discoverable(DiscoverableType.block)
class PostSignal(Block):
    
    port = IntProperty(name='Port', default=8182)
    endpoint = StringProperty(name='Endpoint', default='')

    def __init__(self):
        super().__init__()
        self._server = None
        self._signals = []

    def configure(self, context):
        super().configure(context)
        self._server = WebEngine.create(self.endpoint, 
                                        {'socket_port': self.port})
        self._server.add_handler(BuildSignal(self.notify_signals))
        context.hooks.attach('after_blocks_start', WebEngine.start)
        context.hooks.attach('after_blocks_stop', WebEngine.stop)

