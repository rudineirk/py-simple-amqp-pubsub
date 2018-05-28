from .data import Pipe, Source


class Subscriber:
    def __init__(self, pipe: Pipe):
        self.pipe = pipe
        self.sources = {}

    def listen(self, source: Source, topic: str):
        def decorator(func):
            self._save_event_handler(source, topic, func)
            return func

        return decorator

    def _save_event_handler(self, source: Source, topic: str, func):
        if source not in self.sources:
            self.sources[source] = {}

        self.sources[source][topic] = func.__name__

    def get_event_handlers(self, obj):
        handlers = {}
        for source, topics in self.sources.items():
            handlers[source] = {}
            for topic, func_name in topics.items():
                func = getattr(obj, func_name)
                handlers[source][topic] = func

        return handlers
