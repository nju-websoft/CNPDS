from .config import LLM_API_KEYS


def _api_key_gen():
    while True:
        for key in LLM_API_KEYS:
            yield key


api_key_gen = _api_key_gen()


def get_api_key():
    return next(api_key_gen)
