from google.cloud import firestore
import json

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
        doc.set({
            'id': status_id,
            'user_id': user_id,
            'user_sn': user_sn,
            'created_at': status.get('created_at'),
            'text': status.get('text'),
            'in_reply_to_status_id': status.get('in_reply_to_status_id'),
            'in_reply_to_user_id': status.get('in_reply_to_user_id'),
            'in_reply_to_screen_name': status.get('in_reply_to_screen_name'),
            'quoted_status_id': status.get('quoted_status_id'),
            'lang': status.get('lang'),
            'json': json.dumps(status),
        })
