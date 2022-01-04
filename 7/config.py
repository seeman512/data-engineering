import yaml


class Config:
    def __init__(self, path, app_name):
        with open(path, 'r') as f:
            self.cfg = yaml.load(f, Loader=yaml.Loader).get(app_name)

    def stock_api(self):
        return self.cfg.get("stock_api")

    def fs(self):
        return self.cfg.get("fs")

    def connections(self):
        return self.cfg.get("connections")

    def db(self):
        return self.cfg.get("db")
