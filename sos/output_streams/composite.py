class CompositeOutputStream:
    def __init__(self, streams=()):
        self.streams = list(streams)

    def add_stream(self, stream):
        self.streams.append(stream)

    def close(self):
        for stream in self.streams:
            stream.close()

    def rotate(self):
        for stream in self.streams:
            stream.rotate()

    def on_status(self, status):
        for stream in self.streams:
            stream.on_status(status)
