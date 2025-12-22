import logging

from app.testing_dependencies import client, db_session


def test_ping_api(client):
    response = client.get('ping')
    logging.info(response.json())
