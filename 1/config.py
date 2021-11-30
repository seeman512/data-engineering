import yaml

class Config:
    def __init__(self, path):
        with open(path, 'r') as f:
            self.__config = yaml.load(f, Loader=yaml.Loader)

    def app_config(self, app):
        return self.__config.get(app)
