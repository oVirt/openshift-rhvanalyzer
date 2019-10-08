#!/usr/bin/env python
import asyncio
import aiohttp
import base64
import json
import os
import logging
import requests
import ssl
import sys
import tar_extractor

from io import BytesIO

from aiokafka import AIOKafkaConsumer, ConsumerRecord, AIOKafkaProducer
from aiokafka.errors import KafkaError

# Setup logging
logging.basicConfig(
    level=logging.WARNING,
    format=(
        "[%(asctime)s] %(levelname)s "
        "[%(name)s.%(funcName)s:%(lineno)d] %(message)s"
    )
)
logger = logging.getLogger('consumer')
logger.setLevel(logging.DEBUG)

# Globals
# Asynchronous event loop
MAIN_LOOP = asyncio.get_event_loop()

# Kafka listener config
SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
CONSUMER_TOPIC = os.environ.get('KAFKA_CONSUMER_TOPIC')
PRODUCER_TOPIC = os.environ.get('KAFKA_PRODUCER_TOPIC')

CONSUMER = AIOKafkaConsumer(
    CONSUMER_TOPIC,
    loop=MAIN_LOOP,
    bootstrap_servers=SERVER
)

PRODUCER = AIOKafkaProducer(loop=MAIN_LOOP, bootstrap_servers=SERVER)

# Properties required to be present in a message
VALIDATE_PRESENCE = {'url'}
MAX_RETRIES = 3


def create_host(
    account_number,
    insights_id,
    bios_uuid,
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
        "bios_uuid": bios_uuid,
        "fqdn": fqdn,
        "display_name": fqdn,
        "ip_addresses": ip_addresses
    }

    logger.info("payload: {0}".format(payload))
    json_payload = json.dumps([payload])
    logger.info(json_payload)

    logger.info(URL)
    r = requests.post(URL, data=json_payload, headers=headers, verify=False)
    logger.info("response: {0}".format(r.text))
    logger.info("status_code {0}".format(r.status_code))

    results = json.loads(r.text)
    return results["data"][0]["host"]["id"]


async def init_kafka_resources() -> None:
    """Initialize Kafka resources.
    Connects to Kafka server, consumes a topic and schedules a task for
    processing the message.
    Initializes Producer to eventually produce a message
    :return None
    """
    logger.info('Connecting to Kafka server...')
    logger.info('Consumer configuration:')
    logger.info('\tserver:    %s', SERVER)
    logger.info('\ttopic:     %s', CONSUMER_TOPIC)

    logger.info('Producer configuration:')
    logger.info('\tserver:    %s', SERVER)
    logger.info('\ttopic:     %s', PRODUCER_TOPIC)

    # Get cluster layout, subscribe to group
    await CONSUMER.start()
    logger.info('Consumer subscribed and active!')

    await PRODUCER.start()
    logger.info('Producer all set to produce!')

    # Start consuming messages
    try:
        async for msg in CONSUMER:
            logger.debug('Received message: %s', str(msg))
            MAIN_LOOP.create_task(process_message(msg))

    finally:
        await CONSUMER.stop()
        await PRODUCER.stop()


async def process_message(message: ConsumerRecord) -> bool:
    msg_id = f'#{message.partition}_{message.offset}'
    logger.debug("Receiving message: %s", message)

    try:
        content = json.loads(message.value)
    except ValueError as e:
        logger.error(
            'Unable to parse message %s: %s',
            str(content), str(e)
        )
        return False

    logger.debug('Message %s: %s', msg_id, str(content))

    # Select only the interesting messages
    if not VALIDATE_PRESENCE.issubset(content.keys()):
        return False

    try:
        await recommendations(msg_id, content)
    except aiohttp.ClientError:
        logger.warning('Message %s: Unable to pass message', msg_id)
        return False

    logger.info('Message %s: Done', msg_id)
    return True


async def recommendations(msg_id: str, message: dict):
    """Retrieve recommendations JSON from the TAR file in s3.
    Make an async HTTP GET call to the s3 bucket endpoint
    :param msg_id: Message identifier used in logs
    :param topic: Topic where the message was sent
    :param message: A dictionary sent as a payload
    :return: HTTP response
    """
    # Get json contents from url, which is a tar file
    # Extract it and post the contents on the Advisor topic
    url = message.get('url').strip()
    ssl_context = ssl.SSLContext()
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        for attempt in range(MAX_RETRIES):
            try:
                resp = await session.get(url, ssl=ssl_context)

                data = await resp.read()

                if data:
                    break
            except aiohttp.ClientError as e:
                logger.warning(
                    'Async request failed (attempt #%d), retrying: %s',
                    attempt, str(e)
                )
                resp = e
        else:
            logger.error('All attempts failed!')
            raise resp

    data = await tar_extractor.extract(BytesIO(data))

    # JSON Processing
    hosts = json.loads(data.decode())
    logger.info("==========hosts type==========")
    logger.info(type(hosts))
    logger.info("==========hosts type==========")

    logger.info(hosts)
    for host_info in hosts['rhv-log-collector-analyzer']:
        hits = []
    """
    logger.info("==========host_info type==========")
    logger.info(type(host_info))
    logger.info("==========host_info type==========")
    logger.info("+++++++++++hostinfo++++++++++++++++++")
    logger.info(host_info)
    logger.info("+++++++++++hostinfo++++++++++++++++++")
        #if "rhv-log-collector-analyzer" in host_info:
    """
        hits = await hits_with_rules(hosts)


        host_id = create_host(
            host_info["account"],
            host_info["metadata"]["insights_id"],
            host_info["metadata"]["bios_uuid"],
            host_info["metadata"]["fqdn"],
            host_info["metadata"]["ip_addresses"],
        )
        logger.info("host id: {0}".format(host_id))

        output = {
            'source': 'rhvanalyzer',
            'host_product': 'OCP',
            'host_role': 'Cluster',
            'inventory_id': host_id,
            'account': host_info['account'],
            'hits': hits
        }
        output = json.dumps(output).encode()
        logger.info("JSON {0}".format(output))

    # Produce message constituting the json
    try:
        await PRODUCER.send_and_wait(PRODUCER_TOPIC, output)
        logger.debug("Message %s: produced [%s]", msg_id, output)
    except KafkaError as e:
        logger.debug('Producer send failed: %s', e)

    return resp


async def hits_with_rules(host_info: dict):
    """Populate hits list with rule_id and details."""
    hits = []

    for data in host_info['rhv-log-collector-analyzer']:
        details = {}
        if "WARNING" in data['type'] or "ERROR" in data['type']:
            details.update({
                'description': data['description'],
                'kb': data['kb'],
                'result': data['result']
            })
            logger.info("========== added the following entry ====")
            logger.info("Description: {0}".format(data['description']))
            logger.info("Knowledge Base: {0}".format(data['kb']))
            logger.info("========== added the following entry ====")

            ruleid = data['name']
            hits.append(
                {'rule_id': ruleid + "|" + ruleid.upper(), 'details': details}
            )

    return hits


if __name__ == '__main__':
    # Check environment variables passed to container
    env = {
        'KAFKA_BOOTSTRAP_SERVERS',
        'KAFKA_CONSUMER_TOPIC',
        'KAFKA_PRODUCER_TOPIC'
    }
    if not env.issubset(os.environ):
        logger.error(
            'Environment not set properly, missing %s',
            env - set(os.environ)
        )
        sys.exit(1)

    MAIN_LOOP.run_until_complete(init_kafka_resources())
