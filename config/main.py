import yaml


class Config:
    def __init__(self, path):
        self.path = path

    def load(self) -> dict:
        with open(self.path, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                raise exc
