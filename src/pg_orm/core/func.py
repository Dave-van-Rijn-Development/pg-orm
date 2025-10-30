from typing import TYPE_CHECKING

from psycopg.sql import Composed, SQL, Literal, Identifier

from pg_orm.core.constants import EXCLUDED
from pg_orm.core.query_clause import QueryClause


def or_(first: QueryClause, *conditions: QueryClause) -> QueryClause:
    first.or_.extend(conditions)
    return first


if TYPE_CHECKING:
    def and_(_: bool, *_other: bool) -> QueryClause:
        ...
else:
    def and_(first: QueryClause, *conditions: QueryClause) -> QueryClause:
        first.and_.extend(conditions)
        return first

if TYPE_CHECKING:
    def not_(_: bool, *_other: bool) -> QueryClause:
        ...
else:
    def not_(first: QueryClause, *other: QueryClause) -> QueryClause:
        first.and_.extend(other)
        return ~first


def excluded(value: str) -> Composed:
    return SQL('{}.{}').format(EXCLUDED, Identifier(value))
