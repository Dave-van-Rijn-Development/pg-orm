import datetime
import json
import re
import uuid
from abc import ABC, abstractmethod
from codecs import BOM_LE
from copy import copy
from enum import Enum
from typing import TypeVar, Any, Type, Self

from psycopg.sql import Composed, SQL, Identifier, Composable, Literal

ColType = TypeVar("ColType")


class ColumnType:
    pg_type = None
    python_type = None

    @staticmethod
    def string_parser(value: Any):
        return str(value)

    def __init__(self, is_encrypted: bool = False):
        self.value: ColType = None
        self._is_encrypted = is_encrypted

    def get_db_type(self) -> Composable:
        return SQL(self.pg_type)

    def parse_value(self, value: Any) -> ColType:
        return value

    def get_value(self) -> ColType | None:
        return self.value

    def parse_to_db(self):
        """
        Parse Python value to database value
        """
        return self.value

    def parse_from_db(self, db_value: Any):
        """
        Parse database value to Python value
        """
        if db_value is not None and type(db_value) != self.python_type:
            db_value = self.python_type(db_value)
        self.value = db_value

    def clone(self) -> "ColumnType":
        clone = self.__class__.__new__(self.__class__)
        clone.python_type = self.python_type
        clone._is_encrypted = self._is_encrypted
        clone.value = copy(self.value)
        return clone


class RelationColumnType(ColumnType):
    pg_type = 'Relationship'
    python_type = None


class ForeignColumnType(ColumnType):
    pg_type = 'ForeignKey'
    python_type = None


class String(ColumnType):
    pg_type = 'TEXT'
    python_type = str


class UUID(ColumnType):
    pg_type = 'UUID'
    python_type = uuid.UUID

    def parse_to_db(self):
        value = self.get_value()
        return str(value) if value is not None else None


class Integer(ColumnType):
    pg_type = 'INTEGER'
    python_type = int


class BigInteger(ColumnType):
    pg_type = 'BIGINT'
    python_type = int


class Float(ColumnType):
    pg_type = 'NUMERIC'
    python_type = float


class Date(ColumnType):
    pg_type = 'DATE'
    python_type = datetime.date


class DateTime(ColumnType):
    pg_type = 'TIMESTAMP'
    python_type = datetime.datetime


class Boolean(ColumnType):
    pg_type = 'BOOLEAN'
    python_type = bool


class JSONB(ColumnType):
    pg_type = 'JSONB'
    python_type = dict

    @staticmethod
    def string_parser(value: Any) -> str:
        return json.dumps(value)

    def parse_value(self, value: Any) -> ColType:
        if isinstance(value, str):
            return json.loads(value)
        return value

    def get_value(self) -> dict | None:
        return self.value

    def parse_to_db(self):
        return self.value

    def parse_from_db(self, db_value: Any):
        if self._is_encrypted:
            self.value = json.loads(db_value)
        self.value = db_value


class Array(ColumnType):
    def __init__(self, value_type: ColumnType | Type[ColumnType], is_encrypted: bool = False):
        super().__init__(is_encrypted=is_encrypted)
        if callable(value_type):
            value_type = value_type()
        self._value_type = value_type
        self.pg_type = value_type.pg_type + '[]'
        self.python_type = list[value_type.python_type]

    def parse_to_db(self):
        """
        Parse Python value to database value
        """
        if self.value is None:
            return self.value
        parsed = list()
        for item in self.value:
            if isinstance(item, ColumnType):
                parsed.append(item.parse_to_db())
            elif isinstance(item, Enum):
                parsed.append(item.value)
            else:
                parsed.append(item)
        return parsed

    def parse_from_db(self, db_value: list):
        """
        Parse database value to Python value
        """
        if isinstance(db_value, str):
            # {a,b,c...}
            db_value = re.sub(r'[{}]', '', db_value).split(',')
        if db_value is not None:
            for ix, value in enumerate(db_value):
                self._value_type.parse_from_db(value)
                db_value[ix] = self._value_type.value
        self.value = db_value

    def clone(self) -> "ColumnType":
        clone = super().clone()
        clone._value_type = copy(self._value_type)
        clone.pg_type = copy(self.pg_type)
        return clone


class PGType(ColumnType, ABC):
    name = None

    @abstractmethod
    def build_create_sql(self) -> Composed:
        raise NotImplementedError

    @abstractmethod
    def build_drop_sql(self) -> Composed:
        raise NotImplementedError

    @abstractmethod
    def get_db_type(self) -> Composable:
        raise NotImplementedError

    def clone(self) -> Self:
        clone = super().clone()
        clone.name = self.name
        return clone


class ENUM(PGType):
    pg_type = 'ENUM'
    python_type = Enum

    def __init__(self, name: str, python_type: Type[Enum], **kwargs):
        super().__init__(**kwargs)
        self.python_type = python_type
        self.name = name

    @staticmethod
    def string_parser(value: Enum) -> str:
        return value.value

    def parse_value(self, value: Any) -> ColType:
        if isinstance(value, str):
            return self.python_type(value)
        elif not isinstance(value, Enum):
            raise TypeError(f'Invalid type {type(value)} for column type ENUM')
        return value

    def get_value(self) -> Enum | None:
        return self.value

    def parse_to_db(self):
        if value := self.value:
            return value.value
        return None

    def parse_from_db(self, db_value: Any):
        self.value = self.parse_value(db_value)

    def build_create_sql(self) -> Composed:
        sql = SQL(
            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_type t LEFT JOIN pg_namespace p ON t.typnamespace=p.oid WHERE t.typname={} AND p.nspname='public') THEN CREATE TYPE {} AS ENUM ({}); END IF; END $$;")
        enum_values = [Literal(str(item.value)) for item in self.python_type]
        return sql.format(self.name, Identifier(self.name), SQL(', ').join(enum_values))

    def build_drop_sql(self) -> Composable:
        sql = SQL(
            "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_type t LEFT JOIN pg_namespace p ON t.typnamespace=p.oid WHERE t.typname={} AND p.nspname='public') THEN DROP TYPE {}; END IF; END $$;")
        return sql.format(self.name, Identifier(self.name))

    def get_db_type(self) -> Composable:
        if not self.name:
            raise ValueError('ENUM name is not set')
        return Identifier(self.name)
