import functools
from logger import Logger
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan
from elasticsearch.exceptions import ConnectionError, NotFoundError, ConflictError, RequestError

logs = Logger(__name__)


def handle_elasticsearch_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except NotFoundError as e:
            logs.error(f"Index or document not found: {e}")
        except RequestError as e:
            logs.error(f"Invalid request: {e}")
        except ConnectionError as e:
            logs.error(f"Connection failed: {e}")
        except Exception as e:
            logs.error(f"General errors: {e}")

    return wrapper


class ElasticsearchClient:
    def __init__(self, path, username, password):
        self.path = path
        self.username = username
        self.password = password
        self.es = None
        self.connect_to_elastic(path=path, username=username, password=password)

    @handle_elasticsearch_errors
    def connect_to_elastic(self, path, username, password):
        self.es = Elasticsearch(
            path,
            verify_certs=False,
            basic_auth=(username, password))
        if self.es.ping():
            logs.info("Connected to Elasticsearch cluster")

    @handle_elasticsearch_errors
    def create_index(self, index_name, body):
        response = self.es.indices.create(index=index_name, body=body)
        print(response)

    @handle_elasticsearch_errors
    def index_document(self, index_name, doc_id, body):
        response = self.es.index(index=index_name, id=doc_id, body=body)
        print(response)

    @handle_elasticsearch_errors
    def get_document(self, index_name, doc_id):
        response = self.es.get(index=index_name, id=doc_id)
        print(response['_source'])

    @handle_elasticsearch_errors
    def update_document(self, index_name, doc_id, body):
        response = self.es.update(index=index_name, id=doc_id, body=body)
        print(response)

    @handle_elasticsearch_errors
    def delete_document(self, index_name, doc_id):
        response = self.es.delete(index=index_name, id=doc_id)
        print(response)

    @handle_elasticsearch_errors
    def bulk_documents(self, index_name, documents):
        actions = [
            {
                '_index': index_name,
                '_id': doc['id'],
                '_source': doc
            }
            for doc in documents
        ]
        response = bulk(self.es, actions)
        print(response)

    @handle_elasticsearch_errors
    def scan_documents(self, index_name):
        response = scan(self.es, index=index_name)
        for doc in response:
            yield doc['_source']


if __name__ == '__main__':
    Elastic_username = 'elastic'
    Elastic_pwd = 'test_user'
    Elastic_path = 'http://localhost:9200'
    INDEX_NAME = "nba_players"
    elastic_instance = ElasticsearchClient(path=Elastic_path,
                                           username=Elastic_username,
                                           password=Elastic_pwd)
    elastic_instance.delete_document(index_name=INDEX_NAME, doc_id=2)

    generator_from_scan = elastic_instance.scan_documents(index_name=INDEX_NAME)
    for document in generator_from_scan:
        print(document)
