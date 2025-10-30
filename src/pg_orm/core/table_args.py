from abc import ABC, abstractmethod
from typing import MutableMapping

from psycopg.sql import Composed, SQL, Identifier, Composable, Literal

from pg_orm.core.column import Column
from pg_orm.core.enums import IndexOption


class TableArg(ABC):
    def __init__(self, name: str):
        self.name = name
        self.table_name = None

    @abstractmethod
    def build_create_sql(self) -> Composed:
        raise NotImplementedError()

    @abstractmethod
    def build_drop_sql(self) -> Composed:
        raise NotImplementedError()


class Index(TableArg):
    def __init__(self, name: str, *columns: str | Column,
                 options: MutableMapping[str | Column, IndexOption | tuple[IndexOption, ...]] = None):
        super().__init__(name)
        self.columns = columns
        self.options = options

    def build_create_sql(self) -> Composed:
        sql = SQL("CREATE INDEX IF NOT EXISTS {name} ON {table_name} USING btree ({columns});")
        columns: list[Composable] = self._build_columns()
        return sql.format(
            name=Identifier(self.name),
            table_name=Identifier(self.table_name),
            columns=SQL(", ").join(columns),
        )

    def _build_columns(self) -> list[Composable]:
        columns: list[Composable] = list()
        for column in self.columns:
            sql_name = get_column_name(column)
            sql = Identifier(sql_name)
            column_options = self._get_column_options(sql_name=sql_name)
            sql += SQL(" ") + SQL(" ").join(column_options)
            columns.append(sql)
        return columns

    def _get_column_options(self, *, sql_name: str) -> list[Composable]:
        options: list[Composable] = list()
        if not self.options:
            return options
        for key, value in self.options.items():
            _sql_name = get_column_name(key)
            if _sql_name == sql_name:
                if isinstance(value, tuple):
                    for option in value:
                        options.append(SQL(option.value))
                else:
                    options.append(SQL(value.value))
        return options

    def build_drop_sql(self) -> Composed:
        sql = SQL("DROP INDEX IF EXISTS {name};")
        return sql.format(name=Identifier(self.name))


class UniqueConstraint(TableArg):
    def __init__(self, name: str, *columns: str | Column):
        super().__init__(name)
        self.columns = columns

    def build_create_sql(self) -> Composed:
        sql = SQL(
            "DO $$ BEGIN IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage "
            "WHERE table_name = {str_table_name} AND constraint_name = {str_name}) THEN ALTER TABLE IF EXISTS "
            "{table_name} ADD CONSTRAINT {name} UNIQUE ({columns}); END IF; END; $$;")
        # sql = SQL("ALTER TABLE IF EXISTS {table_name} ADD CONSTRAINT {name} UNIQUE ({columns});")
        return sql.format(
            str_table_name=Literal(self.table_name),
            table_name=Identifier(self.table_name),
            str_name=Literal(self.name),
            name=Identifier(self.name),
            columns=SQL(", ").join(Identifier(get_column_name(col)) for col in self.columns),
        )

    def build_drop_sql(self):
        sql = SQL("ALTER TABLE IF EXISTS {table_name} DROP CONSTRAINT IF EXISTS {name};")
        return sql.format(
            table_name=Identifier(self.table_name),
            name=Identifier(self.name),
        )


def get_column_name(column: Column | str) -> str:
    if isinstance(column, Column):
        return column.sql_name()
    return column
