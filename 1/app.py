import os
from dotenv import load_dotenv
from config import Config
from api_client import ApiClient
from api_storage import ApiStorage
import logging


load_dotenv()
config_path = "config.yaml"
app_name = "app"
dates = ["2021-04-01", "2021-04-02", "2021-04-03", "2021-04-04"]


class AppError(Exception):
    pass


if __name__ == "__main__":
    try:
        cfg = Config(config_path)

        logging.basicConfig(**cfg.logger_config())

        app_username = os.getenv("app_username")
        app_password = os.getenv("app_password")
        if not app_username or not app_password:
            raise AppError("Require application username or password in environment")

        app_config = cfg.app_config(app_name)
        app_config['username'] = app_username
        app_config['password'] = app_password

        client = ApiClient(app_config)
        storage = ApiStorage(app_config)

        for date in dates:
            data = client.get_data(date)

            storage.store(date, data)

    except AppError as e:
        logging.error("ERROR", e)
    except Exception as e:
        logging.error(e)
