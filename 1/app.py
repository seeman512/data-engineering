import os
from dotenv import load_dotenv
from config import Config


load_dotenv()
config_path = "config.yaml"
app_name = "app"


class AppError(Exception):
    pass


if __name__ == "__main__":
    try:
        app_username = os.getenv("app_username")
        app_password = os.getenv("app_username")
        if not app_username or not app_password:
            raise AppError("Require application username or password in environment")

        cfg = Config(config_path)
        print(cfg.app_config(app_name))
        
    except AppError as e:
        print("ERROR", e)
    except Exception:
        pass
