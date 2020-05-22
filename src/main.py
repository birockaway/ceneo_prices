import csv
import os
import re
import time
import itertools
import requests
import logging
import datetime
import json
from urllib.parse import urlparse

from keboola import docker
from logstash_formatter import LogstashFormatterV1


def parse_offer(offer_raw):
    offer = offer_raw.copy()
    offer["eshop"] = urlparse(offer.get("CustName", "")).netloc.lower()
    return offer


def parse_product(product_card):
    common_keys = {"product_" + k: v for k, v in product_card.items() if k != "offers"}
    common_keys["cse_url"] = f"https://ceneo.pl/{common_keys.get('CeneoProdID', '')}"

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
        logger.debug(f"Request failed. Exception {e}. Batch ids: {batch_ids}")
        return None

    else:
        if not req.ok:
            logger.debug(f"Request failed. Status: {req.status_code}")
            return None

        req_json = req.json()

        if req_json["Response"]["Status"] != "OK":
            logger.debug(f"Request body returned non-ok status. {req_json}. Batch ids: {batch_ids}")
            return None

        if "product_offers_by_ids" not in req_json["Response"]["Result"].keys():
            logger.debug(f"Request did not return any product data. Result: {req_json}. Batch ids: {batch_ids}")
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


if __name__ == "__main__":

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = LogstashFormatterV1()

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level="DEBUG")

    utctime_started = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    utctime_started_short = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

    kbc_datadir = os.getenv("KBC_DATADIR")
    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()

    # log parameters (excluding sensitive designated by '#')
    logger.info({k: v for k, v in parameters.items() if "#" not in k})

    input_filename = parameters.get("input_filename")
    wanted_columns_mapping = parameters.get("wanted_columns")

    # read unique product ids
    with open(f'{kbc_datadir}in/tables/{input_filename}.csv') as input_file:
        product_ids = {
            str(pid.replace('"', ''))
            for pid
            # read all input file rows, except the header
            in input_file.read().split(os.linesep)[1:]
            if re.match('"[0-9]+"$', pid)
        }

    for batch_i, product_batch in batches(product_ids, batch_size=1000, sleep_time=1):
        logging.info(f"Processing batch: {batch_i}")

        batch_result = scrape_batch(parameters.get("api_url"),
                                    parameters.get("#api_key"),
                                    product_batch)

        results = [
            # filter item columns to only relevant ones and add utctime_started
            {
                **{wanted_columns_mapping[colname]: colval for colname, colval in item.items()
                   if colname in wanted_columns_mapping.keys()},
                **{"ts": utctime_started, "country": "PL", "distrchan": "MA", "source": "ceneo",
                   "source_id": f"ceneo_PL_{utctime_started_short}", "freq": "d"}
            }
            for item
            in batch_result
            # drop products not on ceneo
            if item.get("product_CeneoProdID")
            # drop empty sublists or None results
            if batch_result
        ]

        logger.info(f"Batch {batch_i} results collected. Writing.")

        with open(f"{kbc_datadir}out/files/ceneo_prices_{utctime_started_short}.csv",
                  "a+", encoding="utf-8") as f:
            dict_writer = csv.DictWriter(f, wanted_columns_mapping.values())
            if batch_i == 0:
                dict_writer.writeheader()
            dict_writer.writerows(results)

        logger.info(f"Batch {batch_i} processing finished.")

        logger.info("All batches finished. Writing manifest file.")

        manifest = {
            "is_public": False,
            "is_permanent": False,
            "is_encrypted": False,
            "notify": False,
            "tags": [
                "to_process",
                "ceneo_prices"
            ]
        }
        with open(f"{kbc_datadir}out/files/ceneo_prices_{utctime_started_short}.csv.manifest", "w",
                  encoding="utf-8") as f:
            json.dump(manifest, f)

    logger.info("Finished.")
