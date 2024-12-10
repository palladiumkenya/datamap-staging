import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from settings import settings




log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)




DATABASE_URL = (
    f"mssql+pymssql://{settings.SQL_USER}:{settings.SQL_PASSWORD}@"
    f"{settings.SQL_HOST}:{settings.SQL_PORT}/{settings.SQL_DB}"
)

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Create a SessionLocal class for database sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get a database session
def get_database():
    """
    Dependency that provides a database session.
    Closes the session after the request is completed.
    """
    db = SessionLocal()  # Create a new session
    try:
        yield db
    finally:
        db.close()  # Ensure the session is closed


