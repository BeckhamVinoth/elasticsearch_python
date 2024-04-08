from elasticsearch import Elasticsearch


Elastic_username = 'elastic'
Elastic_pwd = 'test_user'
Elastic_path = 'http://localhost:9200'

Index_Name = 'nba_players'

mapping_data = {
    "first_name": {
        "type": "text"
    },
    "last_name": {
        "type": "text"
    },
    "date_of_birth": {
        "type": "date"
    },
    "position": {
        "type": "keyword"
    },
    "team": {
        "type": "keyword"
    },
    "avg_scoring": {
        "type": "float"
    },
    "avg_rebound": {
        "type": "float"
    },
    "avg_assist": {
        "type": "float"
    },
    "country": {
        "type": "keyword"
    }}


def connect_elastic() -> Elasticsearch:
    client = Elasticsearch(Elastic_path,
                           verify_certs=False,
                           basic_auth=(Elastic_username, Elastic_pwd)
                           )
    return client


def main():
    client = connect_elastic()
    client.indices.create(index=Index_Name,
                          mappings={
                                'properties': mapping_data
                          })


if __name__ == '__main__':
    main()
