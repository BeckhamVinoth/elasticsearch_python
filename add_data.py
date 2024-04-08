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
    for index, doc in enumerate(list_of_nba_players):
        client.index(index=Index_Name, document=doc, id=index)


if __name__ == '__main__':
    main()
