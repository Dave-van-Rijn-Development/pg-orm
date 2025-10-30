from __future__ import annotations

from typing import TYPE_CHECKING

from pg_orm.aio.async_session import AsyncDatabaseSession
from pg_orm.core.column import Relationship, ForeignKey
from psycopg.sql import Identifier, SQL

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel


class AsyncRelationship(Relationship):
    def __init__(self, table_name: str, **kwargs):
        super().__init__(table_name=table_name, **kwargs)

    async def get_value(self, apply_default: bool = True) -> SQLModel:
        return await self._get_from_session()

    async def set_value(self, value: SQLModel):
        if value == await self.get_value(apply_default=False):
            return
        if not (ref_cls := self.ref_table_cls):
            raise ValueError(f'No table class found for table name {self.ref_table_name}')
        ref_id = ref_cls.primary_columns()[0].get_value(apply_default=False)
        if column := self.fk_column:
            column.set_value(ref_id)

    async def _get_from_session(self) -> SQLModel | None:
        if not (registry := self.table_class.registry):
            raise ValueError(f'Table class {self.table_class.__name__} is not registered')
        if not (ref_cls := registry.get_model(model_name=self.ref_table_name)):
            raise ValueError(f'No table class found for table name {self.ref_table_name}')
        session = await AsyncDatabaseSession.create()
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
        return await query.first()
