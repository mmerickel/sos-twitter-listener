from google.cloud import firestore

class GCPFirestoreOutputStream:
    def __init__(self, *, collection, client=None):
        if client is None:
            client = firestore.Client()
        self.collection = client.collection(collection)

    @classmethod
    def from_config(cls, profile, collection):
        return cls(collection=collection)

    def close(self):
        pass

    def rotate(self):
        pass

    def on_status(self, status):
        status_id = status['id']
        user_id = status['user']['id']
        user_sn = status['user']['screen_name']
        doc_name = f'{status_id}-{user_id}-{user_sn}'
        doc = self.collection.document(doc_name)
        doc.set(status)
