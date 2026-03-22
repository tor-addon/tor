
from rapidfuzz import fuzz
from PTT import parse_title

import unicodedata

from pprint import pprint

class StreamFilter:
    def __init__(self, tmdb_info: dict, min_match: float = 85.0, target_season: int = None,
                 target_episode: int = None, target_language: str = None, remove_trash: bool = True):

        self.media_type = tmdb_info.get("type")
        self.tmdb_year = tmdb_info.get("year")
        self.min_match = min_match
        self.target_season = target_season
        self.target_episode = target_episode
        self.target_language = target_language.lower() if target_language else None
        self.remove_trash = remove_trash

        self.tmdb_titles = [self._clean_string(t) for t in tmdb_info.get("titles", []) if t]

    def _clean_string(self, text: str) -> str:
        if not text:
            return ""
        return unicodedata.normalize('NFKD', str(text)).encode('ascii', 'ignore').decode('ascii').strip().lower()

    def process_stream(self, raw_stream: dict) -> bool:
        """
        Processes a stream IN-PLACE.
        Adds 'valid' (bool) and 'invalid_reason' (str) directly to the dictionary.
        Returns True if valid, False otherwise.
        """
        # Safely grab the title, falling back to torrent_name if 'title' is missing
        raw_title = raw_stream.get("title") or raw_stream.get("torrent_name", "")

        mod_title = raw_title.replace("french", "vff").replace("French", "vff").replace("FRENCH", "vff").replace('VOF', 'vff').replace('VOF', 'vff')
        parsed_data = parse_title(mod_title, translate_languages=False)

        # 1. Update dictionary in-place
        raw_stream.update(parsed_data)
        raw_stream["torrent_name"] = raw_title

        # We default to False. It saves us from writing it multiple times.
        raw_stream["valid"] = False

        # ---------------------------------------------------------
        # 2. FAIL-FAST FILTERS
        # ---------------------------------------------------------
        if self.remove_trash and raw_stream.get("trash"):
            raw_stream["invalid_reason"] = "Trash result"
            return False

        if self.target_language:
            stream_langs = raw_stream.get("languages")
            if not stream_langs or not any(str(lang).lower() == self.target_language for lang in stream_langs):
                raw_stream["invalid_reason"] = "Language mismatch"
                return False

        if self.media_type == "movie":
            t_year = raw_stream.get("year")
            if self.tmdb_year and t_year:
                try:
                    if abs(int(t_year) - int(self.tmdb_year)) > 1:
                        raw_stream["invalid_reason"] = "Year mismatch"
                        return False
                except ValueError:
                    pass

        elif self.media_type == "series":
            t_seasons = raw_stream.get("seasons", [])
            t_complete = raw_stream.get("complete", False)

            if self.target_season:
                if self.target_season not in t_seasons and not t_complete:
                    raw_stream["invalid_reason"] = "Season mismatch"
                    return False

                t_episodes = raw_stream.get("episodes", [])
                if self.target_episode and t_episodes and not t_complete:
                    if self.target_episode not in t_episodes:
                        raw_stream["invalid_reason"] = "Episode mismatch"
                        return False

        # ---------------------------------------------------------
        # 3. STRICT TITLE MATCHING
        # ---------------------------------------------------------
        parsed_title = self._clean_string(raw_stream.get("title", ""))
        best_score = 0.0

        for tmdb_title in self.tmdb_titles:
            score = fuzz.ratio(parsed_title, tmdb_title)
            if score > best_score:
                best_score = score
                if best_score == 100.0:
                    break

        #raw_stream["match_score"] = round(best_score, 2)

        if best_score < self.min_match:
            raw_stream["invalid_reason"] = f"Title match too low ({best_score}%)"
            return False

        # ---------------------------------------------------------
        # 4. SIZE FORMATTING & SUCCESS
        # ---------------------------------------------------------
        try:
            bytes_ = int(raw_stream.get("size", 0))
            gb = bytes_ / (1 << 30)
            raw_stream["size_fmt"] = f"{gb:.2f} GB" if gb >= 1 else f"{bytes_ / (1 << 20):.0f} MB"
        except (ValueError, TypeError):
            raw_stream["size_fmt"] = str(raw_stream.get("size", ""))

        # If it survived until here, it's valid!
        raw_stream["valid"] = True
        raw_stream.pop("invalid_reason", None)  # Clean up just in case
        return True

tmdb_info = {'titles': ['Breaking Bad'], 'imdb_id': 'tt0903747', 'tmdb_id': '1396', 'type': 'tv', 'year': 2008}

st = StreamFilter(tmdb_info, 75, target_season=3, target_episode=5, target_language='fr')

stream = {'category': '5000',
 'infohash': '23C0DED41635293F7CFE92776A9FA9EAEFBDA2E9',
 'seeders': 3,
 'size': '60327080245',
 'source': 'Ygg',
 'title': 'Breaking Bad S05 MULTI BluRay1080p x264 - Chris44'}

st.process_stream(stream)

pprint(stream)