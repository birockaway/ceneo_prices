from keboola import docker
import csv
import os
import re
import time
import itertools
import requests
import logging
import datetime


def parse_item(product_card):
    common_keys = {"product_" + k: v for k, v in product_card.items() if k != "offers"}

    if product_card.get("offers") is not None:
        offer_details = product_card["offers"]["offer"]
    else:
        return [common_keys]

    eshop_offers = [{**common_keys, **offer_detail} for offer_detail in offer_details]
    return eshop_offers


def scrape_batch(url, key, batch_ids):
    params = (('apiKey', key),
              ('resultFormatter', 'json'),
              ('shop_product_ids_comma_separated', ",".join(batch_ids))
              )
    try:
        req = requests.get(url, params=params)

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

        result = list(itertools.chain(*[parse_item(item) for item
                                        in req_json["Response"]["Result"]["product_offers_by_ids"][
                                            "product_offers_by_id"]]))

        return result

    except Exception as e:
        logging.debug(f"Request failed. Exception {e}. Batch ids: {batch_ids}")
        return None


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
    utctime_started = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    logging.basicConfig(format='%(name)s, %(asctime)s, %(levelname)s, %(message)s',
                        level=logging.DEBUG)
    kbc_datadir = os.getenv("KBC_DATADIR")
    cfg = docker.Config(kbc_datadir)
    parameters = cfg.get_parameters()

    # log parameters (excluding sensitive designated by '#')
    logging.info({k: v for k, v in parameters.items() if "#" not in k})

    input_filename = parameters.get("input_filename")
    wanted_columns = parameters.get("wanted_columns")

    # read unique product ids
    with open(f'{kbc_datadir}in/tables/{input_filename}.csv') as input_file:
        product_ids = {
            str(pid.replace('"', ''))
            for pid
            # read all input file rows, except the header
            in input_file.read().split(os.linesep)[1:]
            if re.match('"[0-9]+"$', pid)
        }

    result_nested = []

    for batch_i, product_batch in batches(product_ids, batch_size=1000, sleep_time=1):
        logging.info(f"Processing batch: {batch_i}")

        batch_result = scrape_batch(parameters.get("api_url"),
                                    parameters.get("#api_key"),
                                    product_batch)
        result_nested.append(batch_result)

    logging.info("Batch processing finished.")

    results = [
        # filter item columns to only relevant ones and add utctime_started
        {
            **{colname: colval for colname, colval in item.items() if colname in wanted_columns},
            **{'utctime_started': utctime_started}
        }
        for sublist
        in result_nested
        # drop empty sublists or None results
        if sublist
        for item
        in sublist
    ]

    logging.info("Results collected. Writing.")

    with open(f"{kbc_datadir}out/tables/ceneo_prices.csv", 'w') as f:
        dict_writer = csv.DictWriter(f, wanted_columns + ["utctime_started"])
        dict_writer.writeheader()
        dict_writer.writerows(results)

    logging.info("Finished.")
