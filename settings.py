from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_HOST: str
    DB_PORT: int
    DB_DB: str
    DB_USER: str
    DB_PASSWORD: str
    RABBITMQ_URL: str

    class Config:
        env_file = './.env'
        extra = 'ignore'


settings = Settings()
