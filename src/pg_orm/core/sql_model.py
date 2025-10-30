from typing import Type, MutableMapping, Any

from psycopg.sql import SQL, Composable, Identifier, Composed

from pg_orm.aio.async_query import AsyncInsert, AsyncUpdate
from pg_orm.aio.async_session import AsyncDatabaseSession
from pg_orm.core.column import Column, ForeignKey, Relationship
from pg_orm.core.enums import ModelSessionState
from pg_orm.core.query import Update, Insert, Delete
from pg_orm.core.registry import Registry
from pg_orm.core.session import DatabaseSession
from pg_orm.core.table_args import Index, UniqueConstraint


class SQLModel:
    # Flag model as base class, which means it is not a database table itself but is inherited by other models
    __base_class__: bool = False
    __table_name__: str = None
    __table_args__: tuple[Index | UniqueConstraint] = tuple()
    __schema__: str = 'public'
    initialized: bool = False
    registry: Registry = None
    session_state: ModelSessionState = ModelSessionState.NOT_SET

    @classmethod
    def build_from_db(cls, **kwargs):
        obj = object.__new__(cls)
        # Call init to construct instance attributes
        object.__getattribute__(obj, '__init__')()
        obj._build_from_db(**kwargs)
        return obj

    def __init__(self, **kwargs):
        object.__setattr__(self, '_columns', dict())
        object.__setattr__(self, 'apply_defaults', True)
        object.__setattr__(self, 'exists_in_db', False)
        for col_name, col in self.columns().items():
            col.attr_name = col_name
            self._columns[col_name] = col.clone()
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __init_subclass__(cls: Type["SQLModel"], **kwargs):
        if not SQLModel.registry:
            SQLModel.registry = Registry()
        if cls.__table_name__:
            SQLModel.registry.register_model(model_name=cls.__table_name__, model=cls)
        super().__init_subclass__(**kwargs)

    def __getattribute__(self, item):
        for col in object.__getattribute__(self, '_columns').values():
            if col.attr_name == item:
                return col.get_value(apply_default=self.apply_defaults)
        attr = object.__getattribute__(self, item)
        if isinstance(attr, Column):
            return attr.get_value(apply_default=self.apply_defaults)
        return attr

    def __setattr__(self, item, value):
        for col in object.__getattribute__(self, '_columns').values():
            if col.attr_name == item or col.name == item:
                col.set_value(value)
                return
        for col_name, col in self.columns().items():
            if col_name == item or col.sql_name() == item:
                col.set_value(value)
                return
        object.__setattr__(self, item, value)

    @property
    def schema(self) -> str:
        return self.__schema__

    @schema.setter
    def schema(self, value: str):
        self.__schema__ = value

    @classmethod
    def get_schema(cls) -> str:
        return cls.__schema__

    @classmethod
    def set_schema(cls, schema: str):
        cls.__schema__ = schema

    @property
    def primary_str(self) -> str:
        return ''.join([str(column.get_value(apply_default=False)) for column in self.inst_primary_columns.values()])

    def _build_from_db(self, **kwargs):
        self.apply_defaults = False
        self.exists_in_db = True
        columns = self._columns.values()
        for key, value in kwargs.items():
            for col in columns:
                if col.attr_name == key or col.name == key:
                    col.parse_from_db(value)
                    break

    def has_column(self, name: str) -> bool:
        return name in self._columns

    @classmethod
    def columns(cls, clone: bool = True) -> MutableMapping[str, Column]:
        attr_names = cls.__dict__.keys()
        columns: MutableMapping[str, Column] = cls.get_base_columns(clone=clone)
        for attr_name in attr_names:
            if not attr_name:
                continue
            value = object.__getattribute__(cls, attr_name)
            if isinstance(value, Column):
                name = value.name or attr_name
                if clone:
                    value = value.clone()
                columns[name] = value
        return columns

    @classmethod
    def get_base_columns(cls, clone: bool = True) -> MutableMapping[str, Column]:
        for base in cls.mro():
            if base is not cls and base.__base__ is ModelBase:
                base: Type[ModelBase]
                return base.columns(clone=clone)
        return {}

    @property
    def instance_columns(self) -> dict[str, Column]:
        return self._columns

    @classmethod
    def primary_columns(cls) -> list[Column]:
        return [column for column in cls.columns().values() if column.primary_key]

    @property
    def inst_primary_columns(self) -> dict[str, Column]:
        """
        Instance primary columns
        :return:
        """
        return {name: col for name, col in self._columns.items() if col.primary_key}

    @property
    def primary_values(self) -> dict[Column, Any]:
        return {column: column.get_value(apply_default=self.apply_defaults) for column in
                self.inst_primary_columns.values()}

    @classmethod
    def selectable_columns(cls) -> list[Column]:
        return [column for column in cls.columns().values() if not isinstance(column, Relationship)]

    @property
    def inst_selectable_columns(self) -> dict[str, Column]:
        return {name: col for name, col in self.instance_columns.items() if not isinstance(col, Relationship)}

    @classmethod
    def relationships(cls) -> dict[str, Relationship]:
        relationships: dict[str, Relationship] = dict()
        for attr_name, column in cls.columns().items():
            if isinstance(column, Relationship):
                relationships[attr_name] = column
        return relationships

    @property
    def inst_relationships(self) -> dict[str, Relationship]:
        return {name: col for name, col in self._columns if isinstance(col, Relationship)}

    @classmethod
    def foreign_keys(cls) -> dict[str, ForeignKey]:
        fks: dict[str, ForeignKey] = dict()
        for attr_name, column in cls.columns().items():
            if isinstance(column, ForeignKey):
                fks[attr_name] = column
        return fks

    @property
    def inst_foreign_keys(self) -> dict[str, ForeignKey]:
        return {name: col for name, col in self._columns.items() if isinstance(col, ForeignKey)}

    def set_defaults(self):
        for col in self._columns.values():
            col.get_value(apply_default=True)

    @classmethod
    def build_create_sql(cls) -> Composed:
        if not cls.__table_name__:
            raise ValueError(f'Table name is not set for class {cls.__name__}')
        sql_str = SQL('CREATE TABLE IF NOT EXISTS {} (').format(Identifier(cls.__schema__, cls.__table_name__))
        table_attrs: list[Composable] = list()
        primary_keys: list[Composable] = list()
        pre_create_objects: list[Composable] = list()
        for column in cls.columns().values():
            if isinstance(column, Relationship):
                continue
            if not column.sql_name():
                continue
            table_column_str, pre_create = column.table_column_str()
            table_attrs.append(table_column_str)
            pre_create_objects.extend(pre_create)
            if column.primary_key:
                primary_keys.append(Identifier(column.sql_name()))
        if primary_keys:
            table_attrs.append(SQL(' PRIMARY KEY ({})').format(SQL(', ').join(primary_keys)))
        sql_str += SQL(', ').join(table_attrs) + SQL(')')
        if pre_create_objects:
            pre_create_str = SQL('\n').join(pre_create_objects)
            sql_str = pre_create_str + SQL('\n') + sql_str
        return sql_str + SQL(';')

    @classmethod
    def build_drop_sql(cls):
        if not cls.__table_name__:
            raise ValueError(f'Table name is not set for class {cls.__name__}')
        return SQL('DROP TABLE IF EXISTS {} CASCADE;').format(Identifier(cls.__schema__, cls.__table_name__))

    def build_insert(self, *, session: DatabaseSession) -> Insert:
        if not self.__table_name__:
            raise ValueError(f'Table name is not set for class {self.__class__.__name__}')
        column_names: list[Identifier] = list()
        column_values: list[Any] = list()
        for column in self._columns.values():
            if isinstance(column, Relationship) or column.auto_increment:
                continue
            column_names.append(Identifier(column.sql_name()))
            column_values.append(column.parse_to_db())
        return session.insert(self.__class__).columns(*column_names).values(*column_values)

    def build_async_insert(self, *, session: AsyncDatabaseSession) -> AsyncInsert:
        if not self.__table_name__:
            raise ValueError(f'Table name is not set for class {self.__class__.__name__}')
        column_names: list[Identifier] = list()
        column_values: list[Any] = list()
        for column in self._columns.values():
            if isinstance(column, Relationship) or column.auto_increment:
                continue
            column_names.append(Identifier(column.sql_name()))
            column_values.append(column.parse_to_db())
        return session.insert(self.__class__).columns(*column_names).values(*column_values)

    def build_update(self, *, session: DatabaseSession) -> Update | None:
        if not self.__table_name__:
            raise ValueError(f'Table name is not set for class {self.__class__.__name__}')
        primary_columns = self.inst_primary_columns.values()
        primary_values = self._get_update_primary_keys()
        if len(primary_values) != len(primary_columns):
            # Not all primary keys have been set
            return None
        updates, update_columns = self._get_column_updates()
        if not len(updates):
            return None
        updates.update(self._set_update_columns(update_columns=update_columns))
        statement = session.update(self.__class__)
        for column, value in primary_values.items():
            statement = statement.where(column == value)
        statement = statement.set_(updates)
        return statement

    def build_async_update(self, *, session: AsyncDatabaseSession) -> AsyncUpdate | None:
        if not self.__table_name__:
            raise ValueError(f'Table name is not set for class {self.__class__.__name__}')
        primary_columns = self.inst_primary_columns.values()
        primary_values = self._get_update_primary_keys()
        if len(primary_values) != len(primary_columns):
            # Not all primary keys have been set
            return None
        updates, update_columns = self._get_column_updates()
        if not len(updates):
            return None
        updates.update(self._set_update_columns(update_columns=update_columns))
        statement = session.update(self.__class__)
        for column, value in primary_values.items():
            statement = statement.where(column == value)
        statement = statement.set_(updates)
        return statement

    def _get_column_updates(self) -> tuple[dict[Column, Any], list[Column]]:
        updates: dict[Column, Any] = dict()
        update_columns: list[Column] = list()
        for column in self._columns.values():
            if not column.changed:
                if column.on_update is not None and callable(column.on_update):
                    update_columns.append(column)
                continue
            value = column.parse_to_db(apply_default=False)
            updates[column] = value
        return updates, update_columns

    def _set_update_columns(self, *, update_columns: list[Column]) -> dict[Column, Any]:
        updates: dict[Column, Any] = dict()
        for column in update_columns:
            value = column.on_update()
            updates[column] = value
        return updates

    def _get_update_primary_keys(self) -> dict[Column, Any]:
        primary_columns = self.inst_primary_columns.values()
        primary_values: dict[Column, Any] = dict()
        for column in primary_columns:
            value = column.parse_to_db(apply_default=False)
            primary_values[column] = value
        return primary_values

    def build_delete(self, *, session: DatabaseSession) -> Delete | None:
        if not self.__table_name__:
            raise ValueError(f'Table name is not set for class {self.__class__.__name__}')
        delete = session.delete(self.__class__)
        primary_columns = self.inst_primary_columns.values()
        primary_values: MutableMapping[Column, Any] = dict()
        for column in primary_columns:
            value = column.parse_to_db(apply_default=False)
            primary_values[column] = value
        if len(primary_values) != len(primary_columns):
            # Not all primary keys have been set
            return None
        for column, value in primary_values.items():
            delete = delete.where(column == value)
        return delete

    def object_persisted(self):
        """
        Should be called by the owner Session to notify the object all changes have been pushed to the database
        layer (might not be committed yet though)
        """
        for column in self.columns().values():
            column.changed = False

    def debug_info(self, expand: bool = False, prefix: str = '') -> str:
        """
        Generate a debug string for this object. Includes columns and values and optionally relationships
        :param expand: Load (and debug) relations
        :param prefix: Optional prefix, used in relationships
        """
        debug_str = (f'<{self.__class__.__name__} at {hex(id(self))}>'
                     f'\n{prefix}{"Table name":<11}: {self.__table_name__}'
                     f'\n{prefix}{"Initialized":<11}: {self.initialized}'
                     f'\n{prefix}{"Registry":<11}: {hex(id(self.registry))}'
                     f'\n{prefix}Columns:\n')
        column_values: list[str] = list()
        for column in self.inst_selectable_columns.values():
            if isinstance(column, ForeignKey):
                continue
            column_values.append(
                f'{prefix}\t{column.attr_name}: (PG type: {column._col_type.get_db_type()}, '
                f'{column._col_type.python_type}) = {column.get_value(apply_default=False)}')
        debug_str += '\n'.join(column_values) + f'\n{prefix}Foreign keys:'
        fk_values: list[str] = list()
        for fk in self.inst_foreign_keys.values():
            fk_values.append(
                f'{prefix}\t{fk.attr_name} ({fk.ref_table_name}.{fk.ref_column_name}) = {fk.get_value(apply_default=False)}')
        if fk_values:
            debug_str += '\n' + '\n'.join(fk_values) + f'\n{prefix}'
        if expand:
            debug_str += f'\n{prefix}Relations:\n'
            relation_values: list[str] = list()
            for attr_name, relation in self.inst_relationships.items():
                relation_values.append(
                    f'{prefix}\t{attr_name:<11}: {relation.get_value(apply_default=False).debug_info(
                        expand=expand, prefix=prefix + "\t")}')
            debug_str += '\n\n'.join(relation_values)
        return debug_str

    def __repr__(self):
        values: list[str] = list()
        for column in self.inst_selectable_columns.values():
            values.append(f'{column.attr_name}={column.get_value(apply_default=False)}')
        repr_str = f'<{self.__class__.__name__} at {hex(id(self))}> values ({", ".join(values)})'
        return repr_str


class ModelBase(SQLModel):
    __base_class__ = True

    def __init_subclass__(cls: Type[SQLModel], **kwargs):
        if not SQLModel.registry:
            SQLModel.registry = Registry()
        cls._add_base_columns(_class=cls)
        if cls.__table_name__:
            SQLModel.registry.register_model(model_name=cls.__table_name__, model=cls)
        super().__init_subclass__(**kwargs)

    @classmethod
    def _add_base_columns(cls, *, _class: Type[SQLModel]):
        for column in cls.columns().values():
            setattr(_class, column.attr_name, column)
