#!/usr/bin/env python
import base64
import json
import os
import logging
import requests
import tarfile
import tempfile


from kafka import KafkaConsumer, KafkaProducer


logging.basicConfig(level=logging.INFO)


def parse_json(jsonfile, host_id):
    with open(jsonfile, 'r') as f:
        json_file = f.read()

    json_data = json.loads(json_file)

    hits = []

    for data in json_data['rhv-log-collector-analyzer']:

        logging.info("Description: {0}".format(data['description']))
        logging.info("Knowledge Base: {0}".format(data['kb']))

        details = {}
        if "WARNING" in data['type'] or "ERROR" in data['type']:
            details.update({
                'description': data['description'],
                'kb': data['kb'],
                'result': data['result']
            })

            ruleid = data['name']
            hits.append(
                {'rule_id': ruleid + "|" + ruleid.upper(), 'details': details}
            )

            logging.info("=========")

    return hits


def create_host(
    account_number,
    insights_id,
    machine_id,
    fqdn,
    ip_addresses
):
    """
    Create/Update host in the inventory
    """
    URL = os.environ.get('INSIGHTS_INVENTORY')

    headers = {'Content-type': 'application/json'}
    identity = {'identity': {'account_number': account_number}}
    headers["x-rh-identity"] = base64.b64encode(json.dumps(identity).encode())

    payload = {
        "account": account_number,
        "insights_id": insights_id,
        "machine_id": machine_id,
        "fqdn": fqdn,
        "display_name": fqdn,
        "ip_addresses": ip_addresses
    }

    logging.info("payload: {0}".format(payload))
    json_payload = json.dumps([payload])
    logging.info(json_payload)

    logging.info(URL)
    r = requests.post(URL, data=json_payload, headers=headers, verify=False)
    logging.info("response: {0}".format(r.text))
    logging.info("status_code {0}".format(r.status_code))

    results = json.loads(r.text)
    return results["data"][0]["host"]["id"]


def untar(fname):
    tar = tarfile.open(fname)
    tar.extractall(path=tempfile.gettempdir())
    tar.close()


if __name__ == '__main__':

    # Debug
    logging.info(
        "Using KAFKA_BOOTSTRAP_SERVERS: {btservers}"
        "KAFKA_CONSUMER_TOPIC: {consumer_topic}".format(
            btservers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
            consumer_topic=os.environ.get('KAFKA_CONSUMER_TOPIC')
        )
    )

    # Setup Consumer
    logging.info("Initiating Kafka connection for Consumer...")
    consumer = KafkaConsumer(
        os.environ.get('KAFKA_CONSUMER_TOPIC'),
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
    )
    logging.info("Kafka consumer: {0}".format(consumer))

    producer = KafkaProducer(bootstrap_servers=os.environ.get(
        'KAFKA_BOOTSTRAP_SERVERS'))

    logging.info("Kafka producer: {0}".format(producer))

    # Consume things
    for msg in consumer:
        logging.info("Receiving message:")
        logging.info(msg)

        record = json.loads(msg.value)

        r = requests.get(record['url'])
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logging.exception(err)

        JSON_TAR_GZ = "{tmpdir}/{filename}-rhaccount{rhaccount}.tar.gz".format(
            tmpdir=tempfile.gettempdir(),
            filename="rhv-log-collector-analyzer",
            rhaccount=record['account']
        )

        logging.info("Writing {fname}...".format(fname=JSON_TAR_GZ))
        with open(JSON_TAR_GZ, 'wb') as f:
            f.write(r.content)

        logging.info("Extracting {fname}...".format(fname=JSON_TAR_GZ))
        untar(JSON_TAR_GZ)

        # Renamed extracted JSON to show RH Account number
        JSON_FILE = "{tmpdir}/rhv_log_collector_analyzer_{rh}.json".format(
            tmpdir=tempfile.gettempdir(),
            rh=record['account']
        )

        os.rename(
            "{tmpdir}/rhv_log_collector_analyzer.json".format(
                tmpdir=tempfile.gettempdir()),
            JSON_FILE
        )
        logging.info("JSON available: {0}".format(JSON_FILE))

        host_id = create_host(
            record["account"],
            record["metadata"]["insights_id"],
            record["metadata"]["machine_id"],
            record["metadata"]["fqdn"],
            record["metadata"]["ip_addresses"],
        )
        logging.info("host id: {0}".format(host_id))

        hits = parse_json(JSON_FILE, host_id)

        logging.info("HITS: {0}".format(hits))

        if hits:
            output = {
                'source': 'rhvanalyzer',
                'host_product': 'OCP',
                'host_role': 'Cluster',
                'inventory_id': host_id,
                'account': record['account'],
                'hits': hits
            }
            logging.info("payload: {0}".format(output))
            output = json.dumps(output).encode()

            logging.info("JSON {0}".format(output))
            producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), output)

        logging.info("Removing {0}".format(JSON_TAR_GZ))
        os.unlink(JSON_TAR_GZ)
