from fastapi import FastAPI
from api.routes import router

app = FastAPI(title="NCAA Halftime Predictor API")

app.include_router(router)
