#############################################################################################
#   FASTAPI API CALCUL
#   version : 1.0.0
#   Projet : Fumiwo
#############################################################################################

import json
import uvicorn
import datetime
from router import calcul
from utils.utils import load_config
from fastapi import Request, FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# path file config
CONFIG_PATH = "config/config.json"
# load config
CONFIG_UVICORN, _, _ = load_config(CONFIG_PATH)


# include API correcteur
app.include_router(calcul.router, tags=['API CALCUL'])
app.mount("/data", StaticFiles(directory="data"), name="data")

if __name__ == '__main__':
    uvicorn. run(
        app, host=CONFIG_UVICORN["HOST"], port=CONFIG_UVICORN["PORT"])
