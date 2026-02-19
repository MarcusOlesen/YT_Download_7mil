from pathlib import Path


def load_env():
    try:
        from dotenv import load_dotenv
    except Exception:
        return False

    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=env_path, override=False)
    return True
