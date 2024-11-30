
import os

# tests have a separate .env file that is already loaded here
if os.getenv("ENV") != "test":
    from dotenv import load_dotenv
    load_dotenv(override=True)

environment = os.getenv("ENV", "dev")

import uvicorn

from fastapi.concurrency import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from fastapi_pagination import add_pagination

from app.logging import logger
from app.database import sessionmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Function that handles startup and shutdown events.
    """
    
    logger.info(f"Starting API with environment {environment}")
    
    yield
    
    # Close the DB connection
    if sessionmanager._engine is not None:
        await sessionmanager.close()
        

def read_version():
    """
    Read the version from the VERSION file.
    """
    with open("VERSION", "r") as version_file:
        return version_file.read().strip()


# Initialize the FastAPI app
app = FastAPI(
    debug=environment == "dev",
    title="OmniAPI",
    version=read_version(),
    lifespan=lifespan,
)

# adding app version to environment. This is used in the version endpoint
os.environ["API_VERSION"] = app.version

# CORS setup

frontend_url = os.environ.get("FRONTEND_URL", None)
if not frontend_url:
    raise Exception("FRONTEND_URL environment variable must be set")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes

# TODO!

# Pagination

add_pagination(app)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=1234)