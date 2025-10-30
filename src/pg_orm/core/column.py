from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from enum import Enum
from typing import Callable, Type, Any, Sequence, TYPE_CHECKING, Self

from pg_orm.core.column_type import ColumnType, ColType, ForeignColumnType, RelationColumnType, Integer, BigInteger
from pg_orm.core.query_clause import QueryClause, Equals, NotIn, In, Between, NotEquals, Greater, GreaterEquals, Less, \
    LessEquals, Is, Alias
from psycopg.sql import Identifier, SQL, Composable, Literal, Placeholder, Composed

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel
from pg_orm.core.encryption import encrypt, decrypt
from pg_orm.core.enums import CascadeAction
from pg_orm.core.session import DatabaseSession
from pg_orm.core.types import Queryable


class Column:
    def __init__(self, col_type: Type[ColumnType] | ColumnType, *, name: str = None,
                 default: Callable[[], ColType] = None, primary_key: bool = False, nullable: bool = True,
                 attr_name: str = '', on_update: Callable[[], Any] = None, auto_increment: bool = False, **kwargs):
        if callable(col_type):
            col_type = col_type()
        if auto_increment and not isinstance(col_type, (Integer, BigInteger)):
            raise TypeError('Can only auto increment Integer or BigInteger')
        self._col_type = col_type
        self.auto_increment = auto_increment
        self._value_set = kwargs.get('value_set', False)
        self.changed = kwargs.get('changed', False)
        self._default = default
        self.on_update = on_update
        self.name = name
        self.primary_key = primary_key
        self.nullable = nullable
        self.attr_name = attr_name
        self.table_name: str = kwargs.get('table_name', '')

        from pg_orm.core.sql_model import SQLModel
        self.table_class: Type[SQLModel] = kwargs.get('table_class', SQLModel)

    def clone(self) -> Self:
        column = self.__class__.__new__(self.__class__)
        column._col_type = self._col_type.clone()
        column.auto_increment = self.auto_increment
        column._value_set = self._value_set
        column.changed = self.changed
        column._default = self._default
        column.name = self.name
        column.primary_key = self.primary_key
        column.nullable = self.nullable
        column.attr_name = self.attr_name
        column.table_name = self.table_name
        column.table_class = self.table_class
        column.on_update = self.on_update
        return column

    @property
    def sequence_name(self) -> str:
        sql_name = self.sql_name()
        return f'{self.table_name}_{sql_name}_seq'

    def get_value(self, apply_default: bool = True) -> ColType:
        value = self._col_type.value
        if value is None:
            if apply_default and self._default is not None:
                value = self._default()
                self._col_type.value = value
        return value

    def set_value(self, value: ColType):
        if value == self.get_value(apply_default=False):
            return
        self._col_type.value = value
        self._value_set = True
        self.changed = True

    def parse_to_db(self, apply_default: bool = True) -> ColType:
        """
        Parse Python value to database value
        :param apply_default:
        """
        # We just call get_value to set a default value if needed
        self.get_value(apply_default=apply_default)
        return self._col_type.parse_to_db()

    def parse_from_db(self, db_value: Any):
        """
        Parse database value to Python value
        """
        self._col_type.parse_from_db(db_value)

    def sql_name(self) -> str:
        return self.name or self.attr_name

    def full_sql_name(self) -> Identifier:
        return Identifier(self.table_name, self.sql_name())

    def table_column_str(self) -> tuple[Composable, list[Composable]]:
        pre_create: list[Composable] = list()
        if self.auto_increment:
            pre_create.append(SQL("CREATE SEQUENCE IF NOT EXISTS public.{sequence_name} INCREMENT 1 START 1 "
                                  "MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1;").format(
                sequence_name=Identifier(self.sequence_name)))
        name = self.sql_name()
        sql_str = SQL('{} {}').format(Identifier(name), self._col_type.get_db_type())
        if not self.nullable:
            sql_str += SQL(' NOT NULL')
        if self.auto_increment:
            sql_str += SQL(' DEFAULT nextval({sequence_name}::regclass)').format(
                sequence_name=self.sequence_name)
        return sql_str, pre_create

    def get_column_type(self) -> ColumnType:
        return self._col_type

    def __eq__(self, other: Queryable):
        return QueryClause(operator=Equals(left=self.full_sql_name(), right=other))

    def __ne__(self, other):
        return QueryClause(operator=NotEquals(left=self.full_sql_name(), right=other))

    def __gt__(self, other):
        return QueryClause(operator=Greater(left=self.full_sql_name(), right=other))

    def __ge__(self, other):
        return QueryClause(operator=GreaterEquals(left=self.full_sql_name(), right=other))

    def __lt__(self, other):
        return QueryClause(operator=Less(left=self.full_sql_name(), right=other))

    def __le__(self, other):
        return QueryClause(operator=LessEquals(left=self.full_sql_name(), right=other))

    def __hash__(self):
        return hash(repr(self))

    def in_(self, other: Sequence):
        return QueryClause(operator=In(left=self.full_sql_name(), right=other))

    def not_in(self, other: Sequence):
        return QueryClause(operator=NotIn(left=self.full_sql_name(), right=other))

    def between(self, value1: date | str | int | float, value2: date | str | int | float):
        return QueryClause(operator=Between(left=self.full_sql_name(), right=value1, right2=value2))

    def as_(self, alias: str):
        return QueryClause(operator=Alias(left=self.full_sql_name(), right=alias))

    def is_(self, other):
        # TODO Test if other = None is correctly converted to IS NULL
        return QueryClause(operator=Is(left=self.full_sql_name(), right=other))
        # if other is None:
        #     return SQL('{} is NULL').format(self.full_sql_name())
        # return SQL('{} is {}').format(self.full_sql_name(), Literal(other))


def _other_value(other) -> tuple[Composable, Any]:
    if isinstance(other, Column):
        return other.full_sql_name(), None
    elif isinstance(other, Enum):
        return Placeholder(), other.value
    return Placeholder(), other


class EncryptedColumn(Column):
    def __init__(self, col_type: Type[ColumnType] | ColumnType, **kwargs):
        super().__init__(col_type, **kwargs)
        self._col_type._is_encrypted = True
        self._col_type.pg_type = 'TEXT'

    def get_value(self, apply_default: bool = True) -> ColType:
        value = self._col_type.get_value()
        if value is None:
            if apply_default and self._default is not None:
                value = self._default()
                self._col_type.value = encrypt(self._col_type.string_parser(value))
            return value
        return self._col_type.parse_value(decrypt(self._col_type.get_value()))

    def set_value(self, value: ColType):
        if value == self.get_value(apply_default=False):
            return
        if value is None:
            super().set_value(None)
        else:
            super().set_value(encrypt(self._col_type.string_parser(value)))

    def parse_from_db(self, db_value: str | None):
        # Keep the value as a string, decryption is done at getattr level
        self._col_type.value = db_value

    def __eq__(self, other: Queryable):
        if isinstance(other, (str, int, float, bool)):
            other = encrypt(other)
        return QueryClause(operator=Equals(left=self.full_sql_name(), right=other))

    def __ne__(self, other):
        if isinstance(other, (str, int, float, bool)):
            other = encrypt(other)
        return SQL('{} != {}').format(self.full_sql_name(), other)

    def __hash__(self):
        return hash(repr(self))


class Constraint(Column, ABC):
    """
    PG constraint base class. If table_dependant is True the constraint will be built after all tables have been built
    and be dropped before dropping any tables.
    """
    table_dependant = False

    @abstractmethod
    def build_create_sql(self) -> Composed:
        raise NotImplementedError

    @abstractmethod
    def build_drop_sql(self) -> Composed:
        raise NotImplementedError

    @abstractmethod
    def get_name(self) -> str:
        raise NotImplementedError

    def clone(self) -> Self:
        clone = super().clone()
        clone.table_dependant = self.table_dependant
        return clone


class ForeignKey(Constraint):
    table_dependant = True

    def __init__(self, table_name: str, column_name: str, *,
                 on_update: CascadeAction = CascadeAction.NO_ACTION,
                 on_delete: CascadeAction = CascadeAction.NO_ACTION, **kwargs):
        super().__init__(ForeignColumnType, **kwargs)
        self.ref_table_name = table_name
        self.ref_column_name = column_name
        self.on_update = on_update
        self.on_delete = on_delete

    def get_name(self) -> str:
        return f'{self.table_name}_{self.sql_name()}_fkey'

    def clone(self) -> Self:
        clone = super().clone()
        clone.ref_table_name = self.ref_table_name
        clone.ref_column_name = self.ref_column_name
        clone.on_update = self.on_update
        clone.on_delete = self.on_delete
        return clone

    def build_create_sql(self) -> Composed:
        sql = SQL(
            "DO $$ BEGIN IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage "
            "WHERE table_name = {str_ref_table} AND constraint_name = {str_name}) THEN ALTER TABLE IF EXISTS "
            "{table_name} ADD CONSTRAINT {name} FOREIGN KEY ({fk_column}) REFERENCES {ref_table} ({ref_column}) "
            "MATCH SIMPLE ON UPDATE {on_update} ON DELETE {on_delete}; END IF; END; $$;")
        return sql.format(
            table_name=Identifier(self.table_name),
            str_name=Literal(self.get_name()),
            name=Identifier(self.get_name()),
            fk_column=Identifier(self.sql_name()),
            str_ref_table=Literal(self.ref_table_name),
            ref_table=Identifier(self.ref_table_name),
            ref_column=Identifier(self.ref_column_name),
            on_update=SQL(self.on_update.value),
            on_delete=SQL(self.on_delete.value)
        )

    def build_drop_sql(self) -> Composed:
        return SQL('ALTER TABLE IF EXISTS {table_name} DROP CONSTRAINT IF EXISTS {constraint_name};').format(
            table_name=Identifier(self.table_name),
            constraint_name=Identifier(self.get_name())
        )

    def table_column_str(self) -> tuple[Composable, list]:
        if not (registry := self.table_class.registry):
            raise ValueError(f'Table class {self.table_class.__name__} is not registered')
        ref_cls = registry.get_model(self.ref_table_name)
        ref_col_type: Any = ref_cls.primary_columns()[0]._col_type.get_db_type()
        name = self.sql_name()
        sql_str = SQL('{} {}').format(Identifier(name), ref_col_type)
        if not self.nullable:
            sql_str += SQL(' NOT NULL')
        return sql_str, list()

    def parse_from_db(self, db_value: Any):
        if not (registry := self.table_class.registry):
            raise ValueError(f'Table class {self.table_class.__name__} is not registered')
        ref_cls = registry.get_model(self.ref_table_name)
        ref_col_type: ColumnType = ref_cls.primary_columns()[0]._col_type
        self._col_type = ref_col_type.clone()
        super().parse_from_db(db_value)


class Relationship(Column):
    def __init__(self, table_name: str, **kwargs):
        super().__init__(RelationColumnType, **kwargs)
        self.ref_table_name = table_name

    def clone(self):
        clone = super().clone()
        clone.ref_table_name = self.ref_table_name
        return clone

    @property
    def ref_table_cls(self) -> Type[SQLModel] | None:
        if not (registry := self.table_class.registry):
            return None
        return registry.get_model(model_name=self.ref_table_name)

    @property
    def fk_column(self) -> ForeignKey | None:
        for column in self.table_class.selectable_columns():
            if not isinstance(column, ForeignKey):
                continue
            if column.ref_table_name == self.ref_table_name:
                return column
        return None

    def get_value(self, apply_default: bool = True) -> SQLModel:
        return self._get_from_session()

    def set_value(self, value: SQLModel):
        if value == self.get_value(apply_default=False):
            return
        if not (ref_cls := self.ref_table_cls):
            raise ValueError(f'No table class found for table name {self.ref_table_name}')
        ref_id = ref_cls.primary_columns()[0].get_value(apply_default=False)
        if column := self.fk_column:
            column.set_value(ref_id)

    def _get_from_session(self) -> SQLModel | None:
        if not (registry := self.table_class.registry):
            raise ValueError(f'Table class {self.table_class.__name__} is not registered')
        if not (ref_cls := registry.get_model(model_name=self.ref_table_name)):
            raise ValueError(f'No table class found for table name {self.ref_table_name}')
        session = DatabaseSession()
        ref_primary_col = ref_cls.primary_columns()[0]
        ref_id: str | None = None
        for column in self.table_class.selectable_columns():
            if not isinstance(column, ForeignKey):
                continue
            if column.ref_table_name == self.ref_table_name:
                ref_id = column.get_value(apply_default=False)
                break
        if not ref_id:
            return None
        for obj in session.known_objects.values():
            if type(obj) is ref_cls:
                for ref_column in obj.inst_primary_columns.values():
                    if ref_column.get_value(apply_default=False) == ref_id:
                        return obj
        if not ref_primary_col:
            return None
        query = session.select(ref_cls).where(SQL('{} = {}').format(Identifier(ref_primary_col.sql_name()), ref_id))
        return query.first()
