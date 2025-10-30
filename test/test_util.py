from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

from pg_orm.core.session import DatabaseSession


class TestSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='testing.ini'
    )

    database_user: str
    database_password: str
    database_host: str
    database_port: int
    database_name: str


def init_session():
    settings = get_test_settings()
    DatabaseSession.configure(username=settings.database_user, password=settings.database_password,
                              database_name=settings.database_name, host=settings.database_host,
                              port=settings.database_port)


@lru_cache
def get_test_settings() -> TestSettings:
    # noinspection PyArgumentList
    return TestSettings()
