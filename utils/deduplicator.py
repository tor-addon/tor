class StreamDeduplicator:
    def __init__(self):
        self.seen_hashes = set()

    def process_stream(self, stream: dict):
        """
        Updates the stream IN-PLACE.
        Sets stream['valid'] to True or False and adds an 'invalid_reason' if needed.
        """
        stream['valid'] = False

        infohash = stream.get('infohash') or stream.get('hash')

        if not infohash:
            stream['invalid_reason'] = "Missing infohash"
            return

        normalized_hash = str(infohash).lower()

        if normalized_hash in self.seen_hashes:
            stream['invalid_reason'] = "Duplicate infohash"
            return

        self.seen_hashes.add(normalized_hash)
        stream['valid'] = True
        stream.pop('invalid_reason', None)
        return









