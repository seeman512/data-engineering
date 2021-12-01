import requests
import logging
import json


class ApiClient:

    def __init__(self, cfg):
        self.cfg = cfg
        self.log = logging.getLogger(__name__)
        self.token = None

    def get_data(self, date):
        if not self.token:
            self.set_token()

        url = self.cfg["url"] + self.cfg["endpoint"]
        data = json.dumps({"date": date})
        headers = {"content-type": "application/json",
                   "Authorization": f"JWT {self.token}"}

        r = requests.get(url, data=data, headers=headers)
        if r.status_code == 401:
            self.log.info("Token expired")
            self.set_token()

            headers["Authorization"] = f"JWT {self.token}"
            r = requests.get(url, data=data, headers=headers)

        r.raise_for_status()
        return(r.json())

    def set_token(self):
        r = requests.post(self.cfg["url"] + self.cfg["auth_endpoint"],
                          data=json.dumps({"username": self.cfg["username"],
                                           "password": self.cfg["password"]}),
                          headers={"content-type": "application/json"})

        r.raise_for_status()

        self.token = r.json()["access_token"]
        self.log.debug("Generated new token:" + self.token)
