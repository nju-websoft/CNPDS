import os

from dotenv import load_dotenv


load_dotenv()

LLM_API_MODEL = os.getenv("LLM_API_MODEL")
LLM_API_BASE = os.getenv("LLM_API_BASE")
LLM_API_KEYS = os.getenv("LLM_API_KEYS").split(",")

RERANK_WINDOW_SIZE = int(os.getenv("RERANK_WINDOW_SIZE"))
RERANK_STEP_SIZE = int(os.getenv("RERANK_STEP_SIZE"))

CACHE_DIR = os.getenv("CACHE_DIR")
CACHE_TIMEOUT = int(os.getenv("CACHE_TIMEOUT"))

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))
DB_USER = os.getenv("DB_USER")
DB_PSWD = os.getenv("DB_PSWD")
DB_NAME = os.getenv("DB_NAME")

TB_DESCRIPTIONS = os.getenv("TB_DESCRIPTIONS")
TB_DESCRIPTIONS_ID = os.getenv("TB_DESCRIPTIONS_ID")
TB_DESCRIPTIONS_DESC = os.getenv("TB_DESCRIPTIONS_DESC")
