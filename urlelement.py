import functools


@functools.total_ordering
class UrlElement:
    def __init__(self, url, priority, timestamp, depth=0):
        self.url = url
        self.priority = priority
        self.creation_timestamp = timestamp
        self.depth = depth
        return

    def __eq__(self, other):
        return self.url == other.url

    def __lt__(self, other):
        if self.priority < other.priority:
            return True
        elif self.priority == other.priority:
            return self.creation_timestamp < other.creation_timestamp
        else:
            return False
