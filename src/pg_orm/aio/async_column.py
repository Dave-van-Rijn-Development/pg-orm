from __future__ import annotations

from typing import TYPE_CHECKING, Type

from psycopg.sql import Identifier, SQL

from pg_orm.aio.async_session import AsyncDatabaseSession
from pg_orm.core.column import Relationship, ForeignKey

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel


class AsyncRelationship(Relationship):
    def __init__(self, table_name: str, as_list: bool = None, **kwargs):
        super().__init__(table_name=table_name, as_list=as_list, **kwargs)

    async def get_value(self, apply_default: bool = True) -> SQLModel:
        if self._value is not None:
            return self._value
        return await self._get_from_session()

    async def set_value(self, value: SQLModel):
        if value == await self.get_value(apply_default=False):
            return
        if not (ref_cls := self.ref_table_cls):
            raise ValueError(f'No table class found for table name {self.ref_table_name}')
        ref_id = ref_cls.primary_columns()[0].get_value(apply_default=False)
        if column := self.fk_column:
            column.set_value(ref_id)

    async def _get_from_session(self) -> SQLModel | list[SQLModel] | None:
        if not (registry := self.table_class.registry):
            raise ValueError(f'Table class {self.table_class.__name__} is not registered')
        if not (ref_cls := registry.get_model(model_name=self.ref_table_name)):
            raise ValueError(f'No table class found for table name {self.ref_table_name}')
        if fk_column := self.fk_column:
            self._value = await self._get_parent_from_session(fk_column=fk_column, ref_cls=ref_cls)
        elif ref_fk_column := self.ref_fk_column:
            self._value = await self._get_children_from_session(fk_column=ref_fk_column, ref_cls=ref_cls)
        return self._value

    async def _get_parent_from_session(self, *, fk_column: ForeignKey, ref_cls: Type[SQLModel]) -> \
            SQLModel | list[SQLModel] | None:
        if not (ref_id := fk_column.get_value(apply_default=False)):
            return None
        select_list = self._as_list if self._as_list is not None else False
        session = AsyncDatabaseSession()
        if not select_list:
            # Try to get parent from loaded objects
            for obj in session.known_objects.values():
                if type(obj) is not ref_cls:
                    continue
                for ref_column in obj.inst_primary_columns.values():
                    if ref_column.get_value(apply_default=False) == ref_id:
                        return obj
        # Loading as list or not seen yet, load from DB
        if not (ref_primary_col := ref_cls.primary_columns()[0]):
            return None
        query = session.select(ref_cls).where(SQL('{} = {}').format(Identifier(ref_primary_col.sql_name()), ref_id))
        if select_list:
            return await query.all()
        return await query.first()

    async def _get_children_from_session(self, *, fk_column: ForeignKey, ref_cls: Type[SQLModel]) -> \
            SQLModel | list[SQLModel] | None:
        if not (pk_column := self.pk_column):
            return None
        if not (ref_id := pk_column.get_value(apply_default=False)):
            return None
        select_list = self._as_list if self._as_list is not None else True
        session = AsyncDatabaseSession()
        if not select_list:
            # Try to get parent from loaded objects
            for obj in session.known_objects.values():
                if type(obj) is not ref_cls:
                    continue
                for ref_column in obj.inst_foreign_keys().values():
                    if ref_column.get_value(apply_default=False) == ref_id:
                        return obj
                # Loading as list or not seen yet, load from DB
        query = session.select(ref_cls).where(
            SQL('{} = {}').format(Identifier(fk_column.sql_name()), ref_id))
        if select_list:
            return await query.all()
        return await query.first()
