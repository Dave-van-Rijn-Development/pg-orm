from abc import ABC, abstractmethod
from typing import Any

from psycopg.sql import Composable, SQL, Identifier, Placeholder

from pg_orm.core.types import Queryable


class QueryParams(dict):
    def next_param_name(self) -> str:
        param_ix = 0
        while f'param{param_ix}' in self.keys():
            param_ix += 1
        return f'param{param_ix}'

    def set_param(self, value: Any) -> str:
        param_name = self.next_param_name()
        self[param_name] = value
        return param_name


class Operator(ABC):
    def __init__(self, *, left: Any, right: Any, right2: Any = None):
        self.left = left
        self.right = right
        self.right2 = right2

    @abstractmethod
    def parse(self) -> Composable:
        raise NotImplementedError

    def build(self, params: QueryParams) -> Composable:
        left, params = _transform_queryable(value=self.left, params=params)
        right, params = _transform_queryable(value=self.right, params=params)
        return SQL('{} {} {}').format(left, self.parse(), right)


def _transform_queryable(*, value: "Queryable | QueryClause | None", params: QueryParams) -> tuple[Composable, QueryParams]:
    from pg_orm.core.sql_model import SQLModel
    from pg_orm.core.column import Column
    from pg_orm.core.bind_param import BindParam
    if value is None:
        return SQL('NULL'), params
    if isinstance(value, Composable):
        return value, params
    elif value == type(SQLModel):
        return SQL('{}.*').format(Identifier(value.__table_name__)), params
    elif isinstance(value, Column):
        return value.full_sql_name(), params
    elif isinstance(value, QueryClause):
        return value.parse(params=params), params
    elif isinstance(value, BindParam):
        return Placeholder(value.name), params
    else:
        param_name = params.set_param(value)
        return Placeholder(param_name), params


class Equals(Operator):
    def parse(self) -> Composable:
        return SQL('=')


class NotEquals(Operator):
    def parse(self) -> Composable:
        return SQL('!=')


class Greater(Operator):
    def parse(self) -> Composable:
        return SQL('>')


class GreaterEquals(Operator):
    def parse(self) -> Composable:
        return SQL('>=')


class Less(Operator):
    def parse(self) -> Composable:
        return SQL('<')


class LessEquals(Operator):
    def parse(self) -> Composable:
        return SQL('<=')


class In(Operator):
    def parse(self) -> Composable:
        return SQL('IN')


class NotIn(Operator):
    def parse(self) -> Composable:
        return SQL('NOT IN')


class AnyOP(Operator):
    def parse(self) -> Composable:
        return SQL('ANY')


class Like(Operator):
    def parse(self) -> Composable:
        return SQL('LIKE')


class ILike(Operator):
    def parse(self) -> Composable:
        return SQL('ILIKE')


class Is(Operator):
    def parse(self) -> Composable:
        return SQL('IS')


class Alias(Operator):
    def parse(self) -> Composable:
        return SQL('AS')


class Between(Operator):
    def parse(self) -> Composable:
        ...

    def build(self, params: QueryParams) -> Composable:
        if self.right2 is None:
            raise ValueError('right2 cannot be None')
        left, params = _transform_queryable(value=self.left, params=params)
        right1, params = _transform_queryable(value=self.right, params=params)
        right2, params = _transform_queryable(value=self.right2, params=params)
        return SQL('{} BETWEEN {} AND {}').format(left, self.parse(), right1, right2)


class Join(Operator):
    def parse(self) -> Composable:
        ...

    def build(self, params: QueryParams):
        target, params = _transform_queryable(value=self.left, params=params)
        condition, params = _transform_queryable(value=self.right, params=params)
        return SQL('JOIN {} ON {}').format(target, condition)


class QueryClause:
    def __init__(self, operator: Operator):
        self.operator = operator
        self.or_: list[QueryClause] = list()
        self.and_: list[QueryClause] = list()
        self.inverted: bool = False

    def __or__(self, other: "QueryClause"):
        self.or_.append(other)
        return self

    def __and__(self, other: "QueryClause"):
        self.and_.append(other)
        return self

    def __invert__(self):
        self.inverted = True
        return self

    def parse(self, *, params: QueryParams) -> Composable:
        if self.or_:
            sql = SQL('(')
        else:
            sql = SQL('')
        op = self.operator.build(params=params)
        sql += op
        if self.or_:
            sql += SQL(' OR ') + SQL(' OR ').join((or_.parse(params=params) for or_ in self.or_))
            sql += SQL(')')
        if self.and_:
            all_ops: list[Composable] = [and_.parse(params=params) for and_ in self.and_]
            sql += SQL(' AND ') + SQL(' AND ').join(all_ops)
        if self.inverted:
            sql = SQL('NOT ({})').format(sql)
        return sql


class Distinct:
    def __init__(self, *columns: Queryable, on: bool = False):
        self.columns = columns
        self.on = on


def distinct(*columns: Queryable) -> Distinct:
    return Distinct(*columns)
