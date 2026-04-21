from domains.merchant.router import router as merchant_router
from domains.store.router import router as store_router
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Analytics Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(merchant_router)
app.include_router(store_router)


@app.get("/")
def hello():
    return {"Hi": "Ahlan"}
