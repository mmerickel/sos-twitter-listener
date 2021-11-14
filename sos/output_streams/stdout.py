import json

class StdoutOutputStream:
    def close(self):
        pass

    def rotate(self):
        pass

    def on_status(self, status):
        print(json.dumps(status))
