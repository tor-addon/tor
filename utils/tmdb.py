# Author: adam
import requests

class TMDBApi:
    def __init__(self, apikey="eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJlNTkxMmVmOWFhM2IxNzg2Zjk3ZTE1NWY1YmQ3ZjY1MSIsInN1YiI6IjY1M2NjNWUyZTg5NGE2MDBmZjE2N2FmYyIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.xrIXsMFJpI1o1j5g2QpQcFP1X3AfRjFA5FlBFO5Naw8"):
        self.api ="https://api.themoviedb.org/3"
        self.session = requests.Session()

        self.headers = {
            "Authorization": f"Bearer {apikey}",
            "accept": "application/json"
        }


    def fetch_media_info(self, imdb_id):
        url = self.api + f"/find/{imdb_id}?external_source=imdb_id&language=fr-FR"

        r = requests.get(url, headers=self.headers)

        print(r.json())
        data = r.json()['movie_results'][0]

        titles = [data.get('title')]
        if data.get('original_title') != data.get('title'):
            titles.append(data.get('original_title'))

        tmdb_id = data.get('id')
        type = data.get('media_type')

        release_date = data.get("release_date", "")
        year = int(release_date.split("-")[0]) if release_date else None

        return {
            "titles": titles,
            "imdb_id": imdb_id,
            "tmdb_id": str(tmdb_id),
            "type": type,
            "year": year
        }

        # {'titles': ['Breaking Bad'], 'imdb_id': 'tt0903747', 'tmdb_id': '1396', 'type': 'tv', 'year': 2008}


api = TMDBApi()

data = api.fetch_media_info("tt2527338")


print(data)
