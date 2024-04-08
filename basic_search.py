from elasticsearch import Elasticsearch
from list_of_documents import list_of_nba_players

Elastic_username = 'elastic'
Elastic_pwd = 'test_user'
Elastic_path = 'http://localhost:9200'

Index_Name = 'nba_players'


def connect_elastic() -> Elasticsearch:
    client = Elasticsearch(Elastic_path,
                           verify_certs=False,
                           basic_auth=(Elastic_username, Elastic_pwd)
                           )
    return client


def main():
    client = connect_elastic()

    query = {
        "query": {
            "match": {
                "position": "PF"
            }
        }
    }

    response = client.search(index=Index_Name, body=query)

    # Printing only response json
    for hit in response['hits']['hits']:
        print(hit['_source'])


if __name__ == '__main__':
    main()
