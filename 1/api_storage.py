import logging
import json
import os


class ApiStorage:

    def __init__(self, cfg):
        self.cfg = cfg
        self.log = logging.getLogger(__name__)

    def store(self, date, data):
        dir_path = os.path.join(self.cfg["directory"], date)
        os.makedirs(dir_path, exist_ok=True)

        file_name = os.path.join(dir_path, "products.json")

        with open(file_name, "w") as f:
            json.dump(data, f)

        self.log.debug(f"Stored in {file_name} {len(data)} records")
