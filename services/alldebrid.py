

import requests


class AllDebridCacheManager:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.alldebrid.com/v4"
        self.agent = "stremio_addon"
        self.session = requests.Session()

    def update_cache_status(self, torrents):
        """
        Updates the list of torrent dicts directly in memory.
        """
        if not torrents:
            return torrents

        # 1. Map hashes to their objects (Handle multiple objects with same hash)
        hash_map = {}
        for t in torrents:
            # We use .get() to avoid KeyErrors and .lower() for matching
            h = str(t.get('infohash', '')).strip().lower()
            if h:
                if h not in hash_map:
                    hash_map[h] = []
                hash_map[h].append(t)

        unique_hashes = list(hash_map.keys())
        batch_size = 80

        for i in range(0, len(unique_hashes), batch_size):
            batch = unique_hashes[i: i + batch_size]

            data = {
                "agent": self.agent,
                "apikey": self.api_key,
                "magnets[]": batch
            }

            try:
                response = self.session.post(f"{self.base_url}/magnet/upload", data=data)
                resp_json = response.json()

                if resp_json.get("status") == "success":

                    magnets_data = resp_json.get("data", {}).get("magnets", [])
                    ids_to_delete = []

                    for m in magnets_data:

                        # ALLDEBRID MATCHING LOGIC
                        # AD returns the original input in 'magnet' and the real hash in 'hash'
                        ad_hash = str(m.get("hash") or m.get("magnet", "")).strip().lower()
                        is_ready = m.get("ready", False)

                        if "id" in m:
                            ids_to_delete.append(m["id"])

                        # Find the corresponding torrent(s) in our map
                        if ad_hash in hash_map:
                            for torrent_obj in hash_map[ad_hash]:
                                torrent_obj['cached'] = is_ready
                                if not is_ready:
                                    torrent_obj['rank'] = 0
                        else:
                            # Safety: if AD returns a slightly different hash format
                            # we check if any of our keys are IN the returned string
                            for local_hash in hash_map:
                                if local_hash in ad_hash:
                                    for t_obj in hash_map[local_hash]:
                                        t_obj['cached'] = is_ready
                                        if not is_ready:
                                            t_obj['rank'] = 0

                    # 2. Immediate cleanup
                    if ids_to_delete:
                        self.session.post(
                            f"{self.base_url}/magnet/delete",
                            data={"agent": self.agent, "apikey": self.api_key, "ids[]": ids_to_delete}
                        )
                else:
                    # If API returns error (status: error), we mark as not cached
                    self._set_batch_not_cached(batch, hash_map)

            except Exception as e:
                # If network fails, we assume not cached
                self._set_batch_not_cached(batch, hash_map)

        return torrents

    def _set_batch_not_cached(self, batch, hash_map):
        for h in batch:
            if h in hash_map:
                for t_obj in hash_map[h]:
                    t_obj['cached'] = False
                    t_obj['rank'] = 0

    def close(self):
        self.session.close()


# --- EXAMPLE USAGE ---

torrents = [{'audio': ['Dolby Digital'],
 'category': '5000',
 'channels': ['5.1'],
 'codec': 'hevc',
 'complete': True,
 'dubbed': True,
 'episodes': [],
 'group': 'DWS',
 'hdr': ['SDR'],
 'infohash': '12A19FB8361A4B50608720075C5F9B15997F69E5',
 'languages': ['fr'],
 'quality': 'WEBRip',
 'resolution': '2160p',
 'seasons': [],
 'seeders': 12,
 'size': '322218949621',
 'size_fmt': '300.09 GB',
 'source': 'Ygg',
 'title': 'Breaking Bad',
 'torrent_name': 'Breaking.Bad.COMPLETE.MULTi.VFF.2160p.SDR.WEBRip.AC3.5.1.x265-DWS',
 'valid': True}]

ad_manager = AllDebridCacheManager("")
updated_list = ad_manager.update_cache_status(torrents)

print(torrents[0])
