from .mixins.web_server.web_server_block import WebServer
from nio.common.block.base import Block
from nio.common.signal.base import Signal
from nio.common.command import command
from nio.common.command.params.dict import DictParameter
from nio.common.discovery import Discoverable, DiscoverableType
from nio.common.versioning.dependency import DependsOn
from nio.metadata.properties.string import StringProperty
from nio.metadata.properties.int import IntProperty
from nio.modules.web import RESTHandler


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


@command("post", DictParameter("sig"))
@DependsOn("nio.modules.web", "1.0.0")
@Discoverable(DiscoverableType.block)
class PostSignal(Block, WebServer):

    host = StringProperty(title='Host', default='127.0.0.1')
    port = IntProperty(title='Port', default=8182)

    def __init__(self):
        super().__init__()
        self._server = None
        self._signals = []

    def configure(self, context):
        super().configure(context)
        conf = {
            'host': self.host,
            'port': self.port
        }
        self.add_endpoint(
            BuildSignal(self.notify_signals, self._logger),
            conf
        )

    def post(self, sig):
        self.notify_signals([Signal(sig)])
