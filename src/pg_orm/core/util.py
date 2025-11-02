from __future__ import annotations
from typing import Any, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel


def is_sql_model(arg: Any) -> bool:
    from pg_orm.core.sql_model import SQLModel
    from pg_orm.aio.async_sql_model import AsyncSQLModel
    return type(arg) is type and issubclass(arg, (SQLModel, AsyncSQLModel))


def is_model_base(cls: Type[SQLModel]) -> bool:
    from pg_orm.core.sql_model import ModelBase
    from pg_orm.aio.async_sql_model import AsyncModelBase
    return ModelBase in cls.mro() or AsyncModelBase in cls.mro()
