_mime_map = {}


def register_analyser(mime_type, module):
    _mime_map[mime_type] = module


def get_analyser(mime_type):
    return _mime_map.get(mime_type, None)
