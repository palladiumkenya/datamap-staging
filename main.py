from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging
from routes import  staging_api, data_dictionary_api, staging_history_api
from sqlalchemy import (
    Column,
    String,
    Integer,
    Boolean,Float,
    DateTime,
    create_engine,
)
from database import database
from database import *
from models.models import *
from database.database import get_database, SessionLocal, engine
from sqlalchemy.orm import Session


app = FastAPI()

origins = [
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)





def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


#
# # Dynamically create models
# models = create_models_from_metadata()



@app.on_event("startup")
async def startup_event():
    # Create tables
    Base.metadata.create_all(bind=engine)
    log.info("Tables created successfully!")



# app.include_router(access_api.router, tags=['Access'], prefix='/api/db_access')
app.include_router(staging_api.router, tags=['Staging'], prefix='/api/staging')
app.include_router(data_dictionary_api.router, tags=['Data Dictionary'], prefix='/api/data_dictionary')
app.include_router(staging_history_api.router, tags=['Staging History'], prefix='/api/history')


@app.get("/api/staging/healthchecker")
def root():
    return {"message": "Welcome to Datamap staging, we are up and running", "version": "1.000"}


# Run the FastAPI application
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=9000, reload=True)
