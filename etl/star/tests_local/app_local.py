"""App minima para la prueba local: hace el papel del main.py de la compañera e incluye star.py."""
from fastapi import FastAPI

from star import router as star_router

app = FastAPI(title="prueba local del modelo estrella")
app.include_router(star_router)


@app.get("/health")
def health():
    return {"status": "ok"}
