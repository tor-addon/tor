
class StreamRanker:
    # 1. Pre-defined dictionaries for O(1) lookups.
    # The point gaps (80k > 8k > 1k > 100) ensure strict priority:
    # Resolution > Quality > Pack > Size

    RESOLUTION_SCORES = {
        '2160p': 80000,
        '4k': 80000,
        '1080p': 60000,
        '720p': 40000,
        '480p': 20000
    }

    QUALITY_SCORES = {
        'remux': 8000,
        'bluray': 6000,
        'web': 4000,
        'web-dl': 4000,
        'webrip': 4000,
        'hdtv': 2000,
        'cam': -10000  # Huge penalty for CAM/TS
    }

    PACK_SCORE = 1000

    @staticmethod
    def rank(stream: dict) -> dict:
        score = 0

        # --- 1. RESOLUTION ---
        res = stream.get('resolution')
        if res:
            # We use .lower() just in case, and default to 0 if not found
            score += StreamRanker.RESOLUTION_SCORES.get(res.lower(), 0)

        # --- 2. QUALITY ---
        qual = stream.get('quality')
        if qual:
            score += StreamRanker.QUALITY_SCORES.get(qual.lower(), 0)

        # --- 3. PACK CHECK ---
        # A pack is either marked 'complete' OR has seasons but NO episodes
        # Evaluating empty lists like 'not stream.get("episodes")' is highly optimized in Python
        is_complete = stream.get('complete') is True
        has_seasons = bool(stream.get('seasons'))
        has_no_episodes = not stream.get('episodes')

        if is_complete or (has_seasons and has_no_episodes):
            score += StreamRanker.PACK_SCORE

        # --- 4. SIZE ---
        # The size in your JSON is a string ('3116879393'). We convert it to GB.
        # 1 GB = 1073741824 bytes.
        # A 5GB file gives 4 points, a 50GB file gives 46 points.
        # It will NEVER override the PACK_SCORE (1000).
        try:
            size_bytes = int(stream.get('size', 0))
            score += int(size_bytes / 1073741824)
        except (ValueError, TypeError):
            # Failsafe: if size is missing or corrupted, we simply add 0 points
            pass

        # Add the seeders as a tiny micro-bonus to break ties between identical streams
        # 41 seeders = 41 points
        score += int(stream.get('seeders', 0))

        # Update and return the dictionary
        stream['rank'] = score
        return stream


    def sort_ranked_streams(ranked_streams: list) -> list:
        """
        Sorts the list of valid streams based on their 'rank' score in descending order.
        Modifies the list in-place for maximum memory efficiency and returns it.
        """
        # .sort() is faster than sorted() because it doesn't create a new list in memory.
        # It modifies the existing list in-place, which is perfect for our pipeline.
        ranked_streams.sort(key=lambda stream: stream['rank'], reverse=True)

        return ranked_streams





