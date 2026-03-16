"""
config.py — Settings desde .env
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar .env desde el root del proyecto
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")


class Settings:
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    DEBUG: bool = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "data/ci.db")

    @property
    def db_url(self) -> str:
        """Ruta absoluta al archivo SQLite."""
        path = Path(self.DATABASE_PATH)
        if not path.is_absolute():
            path = ROOT_DIR / path
        # Crear directorio si no existe
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)


settings = Settings()
