from __future__ import annotations

from typing import MutableMapping, Type, TYPE_CHECKING, cast

from pg_orm.core.column import Constraint
from pg_orm.core.column_type import PGType

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel, ModelBase
    from pg_orm.aio.async_sql_model import AsyncSQLModel


class Registry:
    def __init__(self):
        self._models: MutableMapping[str, Type[SQLModel | AsyncSQLModel]] = dict()
        self._types: MutableMapping[str, PGType] = dict()
        self._constraints: MutableMapping[str, Constraint] = dict()

    def register_model(self, *, model_name: str, model: Type[SQLModel] | Type[AsyncSQLModel]):
        if model_name in self._models:
            if (current_model := self._models.get(model_name)) is not model:
                raise ValueError(
                    f'Multiple models found for model name {model_name}. Found {current_model} and {model}')
            return
        if not model.initialized:
            self._initialize_model(model)
        self._models[model_name] = model

    def register_type(self, *, pg_type: PGType):
        self._types[pg_type.name] = pg_type

    def register_constraint(self, *, constraint: Constraint):
        self._constraints[constraint.get_name()] = constraint

    def get_model(self, model_name: str) -> Type[SQLModel]:
        if model_name not in self._models:
            raise KeyError(f'No model registered with name {model_name}')
        return self._models.get(model_name)

    def get_models(self) -> MutableMapping[str, Type[SQLModel]]:
        return self._models

    def get_types(self) -> MutableMapping[str, PGType]:
        return self._types

    def get_constraints(self) -> MutableMapping[str, Constraint]:
        return self._constraints

    def _initialize_model(self, cls: Type[SQLModel] | Type[AsyncSQLModel]):
        from pg_orm.core.sql_model import ModelBase
        from pg_orm.aio.async_sql_model import AsyncModelBase
        for mro_class in cls.mro():
            base_class = mro_class.__base__
            if base_class is ModelBase or base_class is AsyncModelBase:
                mro_class: Type[ModelBase]
                _add_base_columns(base_class=mro_class, _class=cls)
        for col_name, col in cls.columns(clone=False).items():
            col.attr_name = col_name
            col.table_name = cls.__table_name__
            col.table_class = cls
            if isinstance(column_type := col.get_column_type(), PGType):
                self.register_type(pg_type=cast(PGType, column_type))
            elif isinstance(col, Constraint):
                self.register_constraint(constraint=col)
        for obj in cls.__table_args__:
            obj.table_name = cls.__table_name__
        cls.initialized = True


def _add_base_columns(*, base_class: Type[ModelBase], _class: Type[SQLModel]):
    for name, column in base_class.columns().items():
        if not name:
            continue
        setattr(_class, column.attr_name, column)
