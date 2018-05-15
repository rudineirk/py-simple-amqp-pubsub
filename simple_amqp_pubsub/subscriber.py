class Subscriber:
    def __init__(self, service: str):
        self.service = service
        self.events = {}

    def listen(self, func_name):
        def decorator(func):
            self._save_event_handler(func_name, func)
            return func

        if isinstance(func_name, str):
            return decorator

        func = func_name
        func_name = func.__name__
        return decorator(func)

    def _save_event_handler(self, event: str, func):
        self.events[event] = func.__name__

    def get_event_handlers(self, obj):
        handlers = {}
        for event, func_name in self.events.items():
            func = getattr(obj, func_name)
            handlers[event] = func

        return handlers
