from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SQL_HOST: str
    SQL_PORT: int
    SQL_DB: str
    SQL_USER: str
    SQL_PASSWORD: str

    class Config:
        env_file = './.env'
        extra = 'ignore'


settings = Settings()
