import csv
import os
import re
import time
import itertools
import requests
import logging
import datetime
from urllib.parse import urlparse
import queue
import threading
import concurrent.futures

from keboola import docker
import logging_gelf.handlers
import logging_gelf.formatters


def parse_offer(offer_raw):
    offer = offer_raw.copy()
    eshop = (urlparse(offer.get("CustName", "")).netloc.lower()
             if urlparse(offer.get("CustName", "")).netloc.lower() != ""
             else offer.get("CustName", "").lower()
             )
    offer["eshop"] = re.sub('(^www.)|(/*)', "", eshop)
    return offer


def parse_product(product_card):
    common_keys = {"product_" + k: v for k, v in product_card.items() if k != "offers"}
    common_keys["cse_url"] = f"https://ceneo.pl/{common_keys.get('product_CeneoProdID', '')}"

    if product_card.get("offers") is not None:
        offer_details = product_card["offers"]["offer"]
    else:
        return [common_keys]

    eshop_offers = [{**common_keys, **parse_offer(offer_detail)} for offer_detail in offer_details]
    return eshop_offers


def scrape_batch(url, key, batch_ids):
    params = (("apiKey", key),
              ("resultFormatter", "json"),
              ("shop_product_ids_comma_separated", ",".join(batch_ids))
              )
    try:
        req = requests.get(url, params=params)

    except Exception as e:
        logging.debug(f"Request failed. Exception {e}. Batch ids: {batch_ids}")
        return None

    else:
        if not req.ok:
            logging.debug(f"Request failed. Status: {req.status_code}")
            return None

        req_json = req.json()

        if req_json["Response"]["Status"] != "OK":
            logging.debug(f"Request body returned non-ok status. {req_json}. Batch ids: {batch_ids}")
            return None

        if "product_offers_by_ids" not in req_json["Response"]["Result"].keys():
            logging.debug(f"Request did not return any product data. Result: {req_json}. Batch ids: {batch_ids}")
            return None

        result = list(itertools.chain(*[parse_product(item) for item
                                        in req_json["Response"]["Result"]["product_offers_by_ids"][
                                            "product_offers_by_id"]]))

        return result


def batches(product_list, batch_size, sleep_time=0):
    prod_batch_generator = (
        (k, [prod_id for _, prod_id in g])
        for k, g
        in itertools.groupby(enumerate(product_list), key=lambda x_: x_[0] // batch_size)
    )

    # yield the first batch without waiting
    yield next(prod_batch_generator, (None, []))

    for batch in prod_batch_generator:
        time.sleep(sleep_time)
        # yield batch for processing
        yield batch


def parse_configs():
    kbc_datadir = os.getenv("KBC_DATADIR", "/data/")
    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()

    # log parameters (excluding sensitive designated by '#')
    logging.info({k: v for k, v in parameters.items() if "#" not in k})

    input_filename = parameters.get("input_filename")

    # read unique product ids
    with open(f'{kbc_datadir}in/tables/{input_filename}.csv') as input_file:
        product_ids = {
            str(pid.replace('"', ''))
            for pid
            # read all input file rows, except the header
            in input_file.read().split(os.linesep)[1:]
            if re.match('"[0-9]+"$', pid)
        }
    return product_ids, parameters


def producer(task_queue):
    utctime_started = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    utctime_started_short = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    product_ids, parameters = parse_configs()
    columns_mapping = parameters.get("columns_mapping")

    for batch_i, product_batch in batches(product_ids, batch_size=1000, sleep_time=1):
        logging.info(f"Processing batch: {batch_i}")

        batch_result = scrape_batch(parameters.get("api_url"),
                                    parameters.get("#api_key"),
                                    product_batch)

        results = [
            # filter item columns to only relevant ones and add utctime_started
            {
                **{columns_mapping[colname]: colval for colname, colval in item.items()
                   if colname in columns_mapping.keys()},
                **{"TS": utctime_started, "COUNTRY": "PL", "DISTRCHAN": "MA", "SOURCE": "ceneo",
                   "SOURCE_ID": f"ceneo_PL_{utctime_started_short}", "FREQ": "d"}
            }
            for item
            in batch_result
            # drop products not on ceneo
            if item.get("product_CeneoProdID")
               # ceneo records sometimes lack eshop name
               and item.get("CustName", "") != ""
               # records without price are useless
               and item.get("Price", "") != ""
            # drop empty sublists or None results
            if batch_result
        ]

        logging.info(f"Batch {batch_i} results collected. Queueing.")

        task_queue.put(results)

    logging.info("Iteration over. Putting DONE to queue.")
    task_queue.put("DONE")


def writer(task_queue, columns_list, threading_event, filepath):
    with open(filepath, 'w+') as outfile:
        results_writer = csv.DictWriter(outfile, fieldnames=columns_list, extrasaction='ignore')
        results_writer.writeheader()
        while not threading_event.is_set():
            chunk = task_queue.get()
            if chunk == 'DONE':
                logging.info('DONE received. Exiting.')
                threading_event.set()
            else:
                results_writer.writerows(chunk)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger()
    try:
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(host=os.getenv('KBC_LOGGER_ADDR'),
                                                                          port=int(os.getenv('KBC_LOGGER_PORT')))
        # remove stdout logging when running inside keboola
        logger.removeHandler(logger.handlers[0])
    except TypeError:
        logging_gelf_handler = logging.StreamHandler()

    logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    colnames = [
        "AVAILABILITY",
        "COUNTRY",
        "CSE_ID",
        "CSE_URL",
        "DISTRCHAN",
        "ESHOP",
        "FREQ",
        "HIGHLIGHTED_POSITION",
        "MATERIAL",
        "POSITION",
        "PRICE",
        "RATING",
        "REVIEW_COUNT",
        "SOURCE",
        "SOURCE_ID",
        "STOCK",
        "TOP",
        "TS",
        "URL",
    ]

    path = f'{os.getenv("KBC_DATADIR")}out/tables/results.csv'

    pipeline = queue.Queue(maxsize=1000)
    event = threading.Event()

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(producer, pipeline)
        executor.submit(writer, pipeline, colnames, event, path)
