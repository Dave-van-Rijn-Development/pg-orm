from __future__ import annotations

from abc import ABC, abstractmethod
from collections import namedtuple
from inspect import isclass
from typing import LiteralString, TYPE_CHECKING, MutableMapping, Any, Generic, TypeVar, Iterator

from psycopg.sql import Composable, Composed, SQL, Identifier, Placeholder, Literal

from pg_orm.core.bind_param import BindParam
from pg_orm.core.query_clause import QueryClause, QueryParams, Join, Distinct, Operator
from pg_orm.core.types import Selectable, Queryable
from pg_orm.core.util import is_sql_model

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel
    from pg_orm.core.column import Column, Relationship
    from pg_orm.core.session import DatabaseSession

RT = TypeVar("RT")


class Query(ABC):
    def __init__(self, *, session: DatabaseSession):
        self._session = session
        self._where: list[QueryClause | Operator | Composable] = list()
        self._target: list[Selectable] = list()
        self._params = QueryParams()

    @abstractmethod
    def parse(self) -> tuple[Composed, QueryParams]:
        raise NotImplementedError

    def from_(self, *target: Selectable) -> Query:
        self._target.extend(target)
        return self

    if TYPE_CHECKING:
        # "Fool" type checkers
        def where(self, *criteria: bool) -> Query:
            ...
    else:
        def where(self, *criteria: QueryClause | Operator | Composable) -> Query:
            self._where.extend(criteria)
            return self

    def first(self):
        if not (result := self._session.execute(self).first()):
            return None
        return self._build_result(result=result)

    def scalar(self):
        self._session.execute(self)
        if not (result := self._session.scalar()):
            return None
        return self._build_result(result=result)

    def fetch_many(self, *, size: int):
        if not (result := self._session.fetch_many(size=size)):
            return None
        return self._build_result(result=result)

    def all(self):
        self._session.execute(self)
        if not (result := self._session.all()):
            return list()
        return [self._build_result(result=res) for res in result]

    def iter_many(self, *, size: int):
        while True:
            if not (result := self._session.fetch_many(size=size)):
                raise StopIteration
            yield self._build_result(result=result)

    def _build_result(self, *, result: dict):
        from pg_orm.core.column import Column
        return_types = self._return_types
        if len(return_types) == 1 or len(set(return_types)) == 1:
            # Test set length to determine if a single type will be built
            return_type = return_types[0]
            if isinstance(return_type, Column):
                return_type = return_type.table_class
            if is_sql_model(return_type):
                obj = return_type.build_from_db(**result)
                if obj.primary_str:
                    self._session.add(obj)
                return obj
            return result
        return self._build_multiple_targets(result=result, return_types=return_types)

    def _build_multiple_targets(self, result: dict, return_types: list):
        from pg_orm.core.sql_model import SQLModel
        from pg_orm.core.column import Column
        ret_objects = dict()
        for ix, (key, value) in enumerate(result.items()):
            ret_type = return_types[ix]
            if isinstance(ret_type, Column):
                ret_type = ret_type.table_class
            ret_type_name = ret_type.__name__
            if ret_type_name not in ret_objects:
                ret_objects[ret_type_name] = ret_type()
            # noinspection PyProtectedMember
            ret_objects[ret_type_name]._build_from_db(**{key: value})
        for obj in ret_objects.values():
            if isinstance(obj, SQLModel):
                self._session.add(obj)
        if len(ret_objects) == 1:
            return list(ret_objects.values())[0]
        return namedtuple('query_result', ret_objects.keys())(**ret_objects)

    @property
    def _return_types(self):
        return self._target

    def _build_where(self):
        if not (where_clause := self._build_parts(parts=self._where, separator=' AND ')):
            return None
        return SQL('WHERE {clause}').format(clause=where_clause)

    def _build_parts(self, parts: list[QueryClause | Queryable] | int | None,
                     separator: LiteralString = ', ', parameterize: bool = True,
                     expand: bool = False) -> Composable | None:
        if parts is None:
            return None
        if isinstance(parts, int):
            return Literal(str(parts))
        if not len(parts):
            return None
        safe_parts: list[Composable] = list()
        for part in parts:
            str_part = self._sql_str(obj=part, parameterize=parameterize, expand=expand)
            safe_parts.append(str_part)
        return SQL(separator).join(safe_parts)

    def _sql_str(self, *, obj: QueryClause | Queryable, parameterize: bool = True,
                 expand: bool = False, full_name: bool = False) -> str | Identifier | Composable | Placeholder:
        from pg_orm.core.column import Column
        from pg_orm.core.sql_model import SQLModel
        if obj is None:
            return SQL('NULL')
        if isinstance(obj, QueryClause):
            return obj.parse(params=self._params)
        elif isinstance(obj, Composable):
            return obj
        elif isinstance(obj, Distinct):
            columns = [self._sql_str(obj=col, parameterize=parameterize, expand=expand, full_name=full_name) for col in
                       obj.columns]
            if obj.on:
                return SQL("DISTINCT ON ({})").format(SQL(", ").join(columns))
            return SQL('DISTINCT {}').format(SQL(", ").join(columns))
        elif isinstance(obj, Operator):
            return obj.build(params=self._params)
        elif isinstance(obj, Column):
            return obj.full_sql_name() if full_name else Identifier(obj.sql_name())
        elif expand:
            columns = obj.selectable_columns()
            return SQL(', ').join([col.full_sql_name() for col in columns])
        elif isclass(obj) and issubclass(obj, SQLModel):
            return SQL('{}.{}').format(Identifier(obj.__schema__), Identifier(obj.__table_name__))
        if parameterize:
            # Untrusted string, parameterize
            if isinstance(obj, BindParam):
                return Placeholder(obj.name)
            param_name = self._params.set_param(obj)
            return Placeholder(param_name)
        return Identifier(obj)

    def __iter__(self):
        self._session.execute(self)
        for item in self._session:
            yield self._build_result(result=item)


class Joinable(Query, ABC):
    def __init__(self, *, session: DatabaseSession):
        super().__init__(session=session)
        self._join: list[QueryClause | Composable] = list()
        self._join_targets: list[Selectable] = list()

    def join(self, obj: Selectable, on: Composable | bool = None) -> Joinable:
        target = obj
        if isinstance(obj, Relationship):
            target = obj.ref_table_cls
            ref_id = obj.fk_column.get_value(apply_default=False)
            if not on:
                ref_column = obj.fk_column.ref_column_name
                for column in target.selectable_columns():
                    if column.sql_name() == ref_column:
                        on = column == ref_id
                        break
        elif isinstance(obj, Column):
            target = obj.table_class
        if target not in self._join_targets:
            self._join_targets.append(target)
        self._join.append(QueryClause(operator=Join(left=obj, right=on)))
        return self

    def outer_join(self, obj: Selectable, on: Composable | bool) -> Joinable:
        target = obj
        if isinstance(obj, Column):
            target = obj.table_class
        if target not in self._join_targets:
            self._join_targets.append(target)
        self._join.append(SQL('LEFT OUTER JOIN {obj} ON {on}').format(obj=Identifier(self._sql_str(obj=obj)), on=on))
        return self

    @property
    def _return_types(self):
        return_types = super()._return_types
        return return_types + self._join_targets


class Executable(Query, ABC):
    def __init__(self, *, session: DatabaseSession):
        super().__init__(session=session)

    def execute(self):
        self._session.execute(self)
        return self


class Returnable(Query, ABC):
    def __init__(self, *, session: DatabaseSession):
        super().__init__(session=session)
        self._returning: list[Selectable] = list()

    def returning(self, *obj: Selectable) -> Returnable:
        self._returning.extend(obj)
        return self

    @property
    def _return_types(self):
        return self._returning

    def _parse_returning(self) -> Composed | None:
        if not self._returning:
            return None
        returning_args: list[Composable] = []
        for arg in self._returning:
            if is_sql_model(arg):
                returning_args.append(SQL('{}.*').format(Identifier(arg.__table_name__)))
            else:
                returning_args.append(self._sql_str(obj=arg))
        return SQL("RETURNING {}").format(SQL(', ').join(returning_args))


class Select(Joinable, Executable, Generic[RT]):
    def __init__(self, *obj: Selectable, session: DatabaseSession):
        super().__init__(session=session)
        self._select: list[Queryable] = list(obj)
        self._group_by: list[Selectable] = list()
        self._order_by: list[Selectable] = list()
        self._limit: int | None = None
        self._offset: int | None = None
        self._as_exists: bool = False
        self._distinct_on: list[Selectable] = list()

    def select(self, *obj: Queryable) -> Select[RT]:
        self._select.extend(obj)
        return self

    def group_by(self, *obj: Selectable) -> Select[RT]:
        self._group_by.extend(obj)
        return self

    def order_by(self, *obj: Selectable) -> Select[RT]:
        self._order_by.extend(obj)
        return self

    def limit(self, limit: int) -> Select[RT]:
        self._limit = limit
        return self

    def offset(self, offset: int) -> Select[RT]:
        self._offset = offset
        return self

    def exists(self) -> bool:
        self._as_exists = True
        return self.scalar()

    def distinct_on(self, *obj: Selectable) -> Select[RT]:
        self._distinct_on = obj
        return self

    def parse(self) -> tuple[Composed, QueryParams]:
        sql_parts: list[Composable] = []

        if select_clause := self._build_parts(parts=self._select, expand=True):
            if self._distinct_on:
                distinct_on_clause = self._build_parts(parts=self._distinct_on)
                statement = SQL("SELECT DISTINCT ON ({clause}) {columns}").format(
                    clause=distinct_on_clause, columns=select_clause)
            else:
                statement = SQL('SELECT {clause}').format(clause=select_clause)
            sql_parts.append(statement)

        if from_clause := self._build_parts(parts=self._target):
            statement = SQL('FROM {clause}').format(clause=from_clause)
            sql_parts.append(statement)
        if self._as_exists:
            self._target = [bool]

        if join_clause := self._build_parts(parts=self._join, separator=' '):
            sql_parts.append(join_clause)

        if where_statement := self._build_where():
            sql_parts.append(where_statement)

        if group_clause := self._build_parts(parts=self._group_by):
            statement = SQL('GROUP BY {clause}').format(clause=group_clause)
            sql_parts.append(statement)

        if order_clause := self._build_parts(parts=self._order_by):
            statement = SQL('ORDER BY {clause}').format(clause=order_clause)
            sql_parts.append(statement)

        if limit_clause := self._build_parts(parts=self._limit):
            statement = SQL('LIMIT {limit}').format(limit=limit_clause)
            sql_parts.append(statement)

        if offset_clause := self._build_parts(parts=self._offset):
            statement = SQL('OFFSET {offset}').format(offset=offset_clause)
            sql_parts.append(statement)

        query = _finalize_query(sql_parts, end_statement=not self._as_exists)
        if self._as_exists:
            query = SQL("SELECT EXISTS ({});").format(query)
        return query, self._params

    @property
    def _return_types(self):
        from pg_orm.core.column import Column
        ret_types = list()
        for obj in self._select:
            if isinstance(obj, Column):
                ret_types.append(obj.table_class)
            else:
                ret_types.append(obj)
        return ret_types

    def __iter__(self) -> Iterator[RT]:
        self._session.execute(self)
        return self._session.__iter__()


class Update(Returnable):
    def __init__(self, *obj: Selectable, session: DatabaseSession):
        super().__init__(session=session)
        self._target: list[Selectable] = list(obj)
        self._set: MutableMapping[Selectable, Any] = dict()

    def set_(self, set_: MutableMapping[Selectable, Any]) -> Update:
        self._set.update(set_)
        return self

    def parse(self) -> tuple[Composed, QueryParams]:
        sql_parts: list[Composable] = []
        if not (update_clause := self._build_parts(parts=self._target)):
            raise ValueError('Update target is not set')
        if not (set_parts := self._parse_set()):
            raise ValueError('No set statement provided')
        statement = SQL('UPDATE {clause}').format(clause=update_clause)
        sql_parts.append(statement)
        sql_parts.append(set_parts)
        if where_statement := self._build_where():
            sql_parts.append(where_statement)
        if returning_statement := self._parse_returning():
            sql_parts.append(returning_statement)
        return _finalize_query(sql_parts), self._params

    def _parse_set(self) -> Composed:
        set_args: list[Composable] = list()
        for key, value in self._set.items():
            sql_key = self._sql_str(obj=key, full_name=False)
            sql_value = self._sql_str(obj=value, full_name=False)
            set_args.append(SQL('{} = {}').format(sql_key, sql_value))
        return SQL('SET ') + SQL(', ').join(set_args)


def _finalize_query(sql_parts: list[Composable], end_statement: bool = True):
    query = SQL(' ').join(sql_parts)
    if end_statement:
        query += SQL(';')
    return query


class Insert(Returnable, Executable):
    def __init__(self, *obj: Selectable | SQLModel, session: DatabaseSession):
        super().__init__(session=session)
        self._target: list[Selectable | SQLModel] = list(obj)
        self._columns: list[Selectable] = list()
        self._values: list[Any] = list()
        self._on_conflict_do_nothing = False
        self._on_conflict_of_constraint: str | None = None
        self._on_conflict_do_update: MutableMapping[Column, Any] | None = None

    def columns(self, *obj: Selectable) -> Insert:
        self._columns.extend(obj)
        return self

    def values(self, *values: Any) -> Insert:
        self._values.extend(values)
        return self

    def on_conflict_do_nothing(self, constraint_name: str = None):
        self._on_conflict_do_nothing = True
        self._on_conflict_of_constraint = constraint_name
        return self

    def on_conflict_do_update(self, constraint_name: str, mapping: MutableMapping[Column, Any]):
        self._on_conflict_do_update = mapping
        self._on_conflict_of_constraint = constraint_name
        return self

    def parse(self) -> tuple[Composed, QueryParams]:
        sql_parts: list[Composable] = []
        if not (target_clause := self._build_parts(parts=self._target)):
            raise ValueError('Insert target is not set')
        sql_parts.append(SQL('INSERT INTO {clause}').format(clause=target_clause))
        if self._columns:
            if columns_clause := self._build_parts(parts=self._columns, parameterize=False):
                sql_parts.append(SQL('({})').format(columns_clause))
        if not (values_statement := self._parse_values()):
            raise ValueError('Insert values is not set')
        sql_parts.append(values_statement)
        if conflict_statement := self._parse_on_conflict():
            sql_parts.append(conflict_statement)
        return _finalize_query(sql_parts), self._params

    def _parse_values(self) -> Composed | None:
        if not self._values:
            return None
        values_clause = self._build_parts(parts=self._values)
        return SQL('VALUES ({})').format(values_clause)

    def _parse_on_conflict(self) -> Composable | None:
        if self._on_conflict_do_nothing:
            if self._on_conflict_of_constraint:
                return SQL("ON CONFLICT ON CONSTRAINT {} DO NOTHING").format(
                    Identifier(self._on_conflict_of_constraint))
            return SQL("ON CONFLICT DO NOTHING")
        if self._on_conflict_do_update and self._on_conflict_of_constraint:
            updates: list[Composed] = list()
            for column, value in self._on_conflict_do_update.items():
                updates.append(SQL("{} = {}").format(Identifier(column.sql_name()), self._sql_str(obj=value)))
            return SQL("ON CONFLICT ON CONSTRAINT {} DO UPDATE SET {}").format(
                Identifier(self._on_conflict_of_constraint),
                SQL(',').join(updates))
        return None


class Delete(Returnable, Executable):
    def __init__(self, *obj: Selectable, session: DatabaseSession):
        super().__init__(session=session)
        self._target: list[Selectable] = list(obj)

    def parse(self) -> tuple[Composed, QueryParams]:
        sql_parts: list[Composable] = []
        if not (target_clause := self._build_parts(parts=self._target)):
            raise ValueError('Delete target is not set')
        sql_parts.append(SQL('DELETE FROM {clause}').format(clause=target_clause))
        if where_statement := self._build_where():
            sql_parts.append(where_statement)
        return _finalize_query(sql_parts), self._params
