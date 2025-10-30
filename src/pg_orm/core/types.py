from typing import Type, TYPE_CHECKING

from psycopg.sql import Composable

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel
    from pg_orm.core.column import Column
    from pg_orm.core.bind_param import BindParam
    from pg_orm.core.query_clause import Distinct, Operator

type Selectable = Type[SQLModel] | Column | Composable | str
type Queryable = Selectable | str | int | float | bool | BindParam | Distinct | Operator
