import yaml


class Config:
    def __init__(self, path):
        with open(path, 'r') as f:
            self.cfg = yaml.load(f, Loader=yaml.Loader)

    def app_config(self, app_name):
        return self.cfg.get(app_name)

    def logger_config(self):
        return self.cfg.get("logger")
