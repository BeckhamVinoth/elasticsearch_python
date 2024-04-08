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
    doc_to_update = {
        "doc": {'first_name': 'LeBron', 'last_name': 'James',
                'date_of_birth': '1984-12-30', 'position': 'PF', 'team': 'Beckham_FC',
                'avg_scoring': 25.4, 'avg_rebound': 7.9, 'avg_assist': 7.9,
                'country': 'USA'}
        }
    client.update(index=Index_Name, id=0, body=doc_to_update)


if __name__ == '__main__':
    main()
