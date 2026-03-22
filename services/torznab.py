# Author: adam


import requests
import xml.etree.ElementTree as ET
import logging

import pprint

class Torznab:
    NS = {'t': 'http://torznab.com/schemas/2015/feed'}

    def __init__(self, name: str, url: str, apikey: str = None):
        self.name = name
        self.url = url
        self.apikey = apikey
        self.session = requests.Session()

    def search(self, query: str = None, categories=None, **kwargs) -> list[dict]:
        params = {'t': 'search', 'limit': 100}

        if self.apikey:
            params['apikey'] = self.apikey

        if query:
            params['q'] = query

        if categories:
            if isinstance(categories, list):
                params['cat'] = ','.join(str(c) for c in categories)
            else:
                params['cat'] = str(categories)

        params.update(kwargs)

        try:
            response = self.session.get(self.url, params=params, timeout=10)
            response.raise_for_status()
            return self._parse(response.text)
        except Exception as e:
            logging.error(f"Torznab {self.name} search error: {e}")
            return []

    def _parse(self, xml_text: str) -> list[dict]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logging.error(f"Torznab {self.name} XML parsing error: {e}")
            return []

        results = []
        for item in root.findall(".//item"):
            data = {
                "title": item.findtext("title"),
                "size": item.findtext("size"),
                "category": item.findtext("category"),
                "source": self.name,
                "seeders": 0,
                "infohash": None
            }

            # Extract specific Torznab attributes
            for attr in item.findall("t:attr", self.NS):
                name = attr.get("name")
                if name == "seeders":
                    value = attr.get("value")
                    data["seeders"] = int(value) if value and value.isdigit() else 0
                elif name == "infohash":
                    data["infohash"] = attr.get("value")

            # Fallback: Sometimes the infohash is embedded in a magnet URI inside the guid
            if not data["infohash"] and data["guid"]:
                guid_lower = data["guid"].lower()
                if "btih:" in guid_lower:
                    # Extract hash from urn:btih:HASH or magnet:?xt=urn:btih:HASH
                    data["infohash"] = guid_lower.split("btih:")[-1].split("&")[0].upper()

            # Optional: Ensure infohash is always uppercase for consistency in Stremio
            if data["infohash"]:
                data["infohash"] = data["infohash"].upper()

            results.append(data)

        return results

t = Torznab("Ygg", "https://relay.ygg.gratis/torznab")
#t = Torznab("C411", "https://c411.org/api", "a3e9340aaf8e07987f64245ff50139bddb4098aebd17b0cd4c4685ac2ef3a85d")

for torrent in t.search("Breaking Bad"):
    pprint.pprint(torrent)









