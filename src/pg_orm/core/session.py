from __future__ import annotations

import atexit
from collections import defaultdict
from threading import local
from typing import Any, Self, TYPE_CHECKING, overload, MutableMapping, Type, Iterable
from weakref import WeakSet

from psycopg import Cursor, Connection
from psycopg.rows import dict_row, tuple_row
from psycopg.sql import Composed, Composable, SQL, Identifier

from pg_orm.core.query_clause import QueryParams, Distinct

if TYPE_CHECKING:
    from pg_orm.core.sql_model import SQLModel
from pg_orm.core.query import Query, Select, Update, Insert, Delete
from pg_orm.core.types import Selectable


class Credentials:
    default_username: str = None
    default_password: str = None
    default_host: str = 'localhost'
    default_port: int = 5432
    default_database_name: str = None

    def __init__(self, *, username: str = None, password: str = None, host: str = None, port: int = None,
                 database_name: str = None):
        self.username = username or Credentials.default_username
        self.password = password or Credentials.default_password
        self.host = host or Credentials.default_host
        self.port = port or Credentials.default_port
        self.database_name = database_name or Credentials.default_database_name


_proxy_attrs = (
    'update',
    'select',
    'execute',
    'execute_many',
    'first',
    'all',
    'scalar',
    'fetch_many',
    'row_count',
    'add',
    'insert',
    'delete',
    'expunge_all',
    'expunge',
    'close',
    'create_all',
    'drop_all',
    'commit',
    'flush',
    'rollback'
    'set_search_path'
)


class SessionMeta(type):
    """
    Metaclass which proxies specific functions to a local session instance.
    This enables the static usage of the session instead of an instance, whilst all operations are performed on
    an actual session instance.
    """
    _configured: bool = False

    def configure(cls, *, username: str, password: str, database_name: str, host: str = 'localhost', port: int = 5432):
        Credentials.default_username = username
        Credentials.default_password = password
        Credentials.default_host = host
        Credentials.default_port = port
        Credentials.default_database_name = database_name
        cls._configured = True

    def __getattribute__(cls, item):
        if item in _proxy_attrs:
            return getattr(DatabaseSession(), item)
        return object.__getattribute__(cls, item)


class DatabaseSession(metaclass=SessionMeta):
    __instances__: defaultdict[type, WeakSet[DatabaseSession]] = defaultdict(WeakSet)
    _auto_commit: bool = True
    # Used to set the search path before each query. This should normally not be needed, but in some cases the
    # search_path gets left behind in an invalid state, like when using postgres_fdw.
    _ensure_path: str | None = None
    known_objects: MutableMapping[str, SQLModel] = dict()
    created_objects: list[SQLModel] = list()
    deleted_objects: MutableMapping[str, SQLModel] = dict()

    def __new__(cls, *, auto_commit: bool = True, credentials: Credentials = None,
                isolate: bool = False, ensure_path: str = None) -> Self:
        """
        Get the DatabaseSession object for the current thread, or construct a new one if none is constructed before.
        This makes use there is generally only one session per thread.

        :param auto_commit: Whether the underlying database connection should autocommit. Default True

        :param credentials: Optional credentials to use for connecting to the database. If not given, uses the default
        configured credentials. This is especially useful for also connection to another database than the default.

        :param isolate: If True, forces a new connection to be constructed which doesn't get shared in the thread.
        """
        if not credentials and not cls._configured:
            raise RuntimeError('Database session must be configured first')
        if not credentials:
            credentials = Credentials()
        if not isolate:
            if (session := _get_local_session(credentials=credentials)) is not None:
                session.auto_commit = auto_commit
                return session
        session = object.__new__(cls)
        session.__connection = None
        session.__cursor = None
        session._auto_commit = auto_commit
        session._ensure_path = ensure_path
        session.known_objects = dict()
        session.deleted_objects = dict()
        session.created_objects = list()
        session.credentials = credentials
        if isolate:
            atexit.register(session.close)
        else:
            _add_local_session(session)
            DatabaseSession.__instances__[cls].add(session)
        session.execute(SQL("SET search_path TO public;"))
        return session

    def test_connect(self):
        return self._connection

    if TYPE_CHECKING:
        @overload
        def select(self: Any, *args, **kwargs) -> Select:
            ...

        @overload
        def update(self: Any, *args, **kwargs) -> Update:
            ...

        @overload
        def execute(self: Any, *args, **kwargs) -> DatabaseSession:
            ...

        @overload
        def execute_many(self: Any, *args, **kwargs) -> DatabaseSession:
            ...

        @overload
        def first(self: Any = None) -> dict[str, Any]:
            ...

        @overload
        def all(self: Any = None) -> list[dict[str, Any]]:
            ...

        @overload
        def scalar(self: Any = None) -> Any:
            ...

        @overload
        def fetch_many(self: Any, *args, **kwargs):
            ...

        @overload
        def row_count(self: Any = None) -> int:
            ...

        @overload
        def add(self: Any, *args, **kwargs) -> DatabaseSession:
            ...

        @overload
        def add_all(self: Any, *args, **kwargs) -> DatabaseSession:
            ...

        @overload
        def insert(self: Any, *args, **kwargs) -> Insert:
            ...

        @overload
        def delete(self: Any, *args, **kwargs) -> Delete:
            ...

        @overload
        def execute_delete(self: Any, *args, **kwargs) -> Delete:
            ...

        @overload
        def expunge_all(self: Any = None) -> DatabaseSession:
            ...

        @overload
        def expunge(self: Any, *args, **kwargs) -> DatabaseSession:
            ...

        @overload
        def close(self: Any = None) -> DatabaseSession:
            ...

        @overload
        def create_all(self: Any = None) -> DatabaseSession:
            ...

        @overload
        def drop_all(self: Any = None) -> DatabaseSession:
            ...

        @overload
        def commit(self: Any = None) -> DatabaseSession:
            ...

        @overload
        def flush(self: Any = None) -> DatabaseSession:
            ...

        @overload
        def rollback(self: Any = None) -> DatabaseSession:
            ...

        @overload
        def set_search_path(self: Any, *args, **kwargs) -> DatabaseSession:
            ...

    def select(self, *obj_type: Selectable) -> Select:
        from pg_orm.core.column import Column
        statement = Select(*obj_type, session=self)
        tables = set()
        for obj in obj_type:
            if isinstance(obj, Column):
                tables.add(obj.table_class)
            elif isinstance(obj, Distinct):
                for col in obj.columns:
                    if isinstance(col, Column):
                        tables.add(col.table_class)
                    elif isinstance(obj, Composable):
                        continue
                    else:
                        tables.add(col)
            elif isinstance(obj, Composable):
                # Target has to be set manually when selecting composables
                continue
            else:
                tables.add(obj)
        if len(tables) == 1:
            statement = statement.from_(tables.pop())
        return statement

    def update(self, *obj_type: Selectable) -> Update:
        return Update(*obj_type, session=self)

    def execute(self, sql: Composed | Query, params: QueryParams = None) -> Self:
        if isinstance(sql, Query):
            sql, _params = sql.parse()
            params = (params or QueryParams()) | _params
        self.set_search_path()
        self._cursor.execute(sql, params)
        return self

    def execute_many(self, sql: Composed | Query, params: Iterable[QueryParams] = None) -> Self:
        if isinstance(sql, Query):
            sql, _params = sql.parse()
            params = [param | _params for param in (params or list())]
        self.set_search_path()
        self._cursor.executemany(sql, params)

    def first(self=None) -> dict[str, Any]:
        return self._cursor.fetchone()

    def all(self=None) -> list[dict[str, Any]]:
        return self._cursor.fetchall()

    def scalar(self=None):
        current_factory = self._cursor.row_factory
        self._cursor.row_factory = tuple_row
        result = self._cursor.fetchone()
        self._cursor.row_factory = current_factory
        if not result:
            return None
        return result[0]

    def fetch_many(self, size: int):
        return self._cursor.fetchmany(size)

    def row_count(self=None) -> int:
        return self._cursor.rowcount

    def add(self, obj: SQLModel) -> Self:
        """
        Add given object to the Python session. This does not insert the object into the database directly,
        but will insert/update the object when the session flushes.
        :param obj:
        :return:
        """
        # Create the cursor if needed to make sure objects get flushed
        _ = self._cursor
        if not obj.exists_in_db:
            obj.set_defaults()
        if primary_str := obj.primary_str:
            self.known_objects[primary_str] = obj
        else:
            self.created_objects.append(obj)
        return self

    def add_all(self, objs: Iterable[SQLModel]) -> Self:
        for obj in objs:
            self.add(obj)
        return self

    def insert(self, obj: Selectable | SQLModel) -> Insert:
        from pg_orm.core.sql_model import SQLModel
        if isinstance(obj, SQLModel):
            return obj.build_insert(session=self)
        return Insert(obj, session=self)

    def delete(self, obj: SQLModel) -> Self:
        primary_str = obj.primary_str
        if primary_str in self.known_objects:
            del self.known_objects[primary_str]
        try:
            self.created_objects.remove(obj)
        except ValueError:
            pass
        if primary_str in self.deleted_objects:
            return self
        self.deleted_objects[obj.primary_str] = obj

    def execute_delete(self, obj: SQLModel) -> Delete:
        """
        Immediately emit a DELETE statement to the database.
        :param obj:
        :return:
        """
        # TODO This should add the object to the deletion list, rename this to execute_delete
        delete = obj.build_delete(session=self)
        return delete

    def set_search_path(self, search_path: str = None) -> Self:
        if not search_path:
            if not self._ensure_path:
                return self
            search_path = self._ensure_path
        self._cursor.execute(SQL("SET search_path TO {search_path};").format(search_path=Identifier(search_path)))
        return self

    def _replace(self, obj: SQLModel) -> Self:
        if not (primary_str := obj.primary_str):
            return self
        self.known_objects[primary_str] = obj
        return self

    @property
    def _connection(self) -> Connection:
        credentials = self.credentials
        if self.__connection is None or self.__connection.closed:
            self.__connection = Connection.connect(user=credentials.username, password=credentials.password,
                                                   host=credentials.host, port=credentials.port,
                                                   dbname=credentials.database_name, autocommit=self._auto_commit)
            self.known_objects = dict()
            self.created_objects = []
            self.deleted_objects = dict()
        return self.__connection

    @property
    def _cursor(self) -> Cursor:
        if not self.__cursor or self.__cursor.closed:
            self.__cursor = self._connection.cursor(row_factory=dict_row)
        return self.__cursor

    @property
    def auto_commit(self):
        return self._auto_commit

    @auto_commit.setter
    def auto_commit(self, auto_commit: bool):
        if auto_commit == self._auto_commit:
            return
        self._auto_commit = auto_commit
        if self.__connection and not self.__connection.closed:
            self.__connection.autocommit = auto_commit

    @property
    def connection_closed(self) -> bool:
        return not self.__connection or self.__connection.closed

    def expunge_all(self=None) -> Self:
        self.known_objects.clear()
        self.created_objects.clear()
        self.deleted_objects.clear()
        return self

    def expunge(self, obj: SQLModel) -> Self:
        if (primary_str := obj.primary_str) in self.known_objects:
            del self.known_objects[primary_str]
        else:
            obj_id = None
            if obj.has_column('id'):
                obj_id = obj.id
            if obj_id:
                for primary_str, _obj in self.known_objects.items():
                    if not _obj.has_column('id'):
                        continue
                    if obj_id == _obj.id:
                        del self.known_objects[primary_str]
                        break
        return self

    def close(self=None):
        if self.__cursor and not self.__cursor.closed:
            self.commit()
            self.__cursor.close()
        if self.__connection and not self.__connection.closed:
            self.__connection.close()
        return self

    def create_all(self=None):
        from pg_orm.core.sql_model import SQLModel
        with self._connection.transaction():
            # Create all required types first, before creating the tables
            self._create_types()
            for _class in SQLModel.registry.get_models().values():
                self._create_class(_class=_class)
            self._create_constraints()
            self._create_table_args()
        return self

    def _create_types(self):
        from pg_orm.core.sql_model import SQLModel
        for _type in SQLModel.registry.get_types().values():
            self.execute(_type.build_create_sql())

    def _create_class(self, *, _class: Type[SQLModel]):
        if _class.__table_name__:
            self.execute(_class.build_create_sql())
        if _class.__base_class__:
            for _sub_class in _class.__subclasses__():
                self._create_class(_class=_sub_class)

    def _create_constraints(self):
        from pg_orm.core.sql_model import SQLModel
        for constraint in SQLModel.registry.get_constraints().values():
            self.execute(constraint.build_create_sql())

    def _create_table_args(self):
        from pg_orm.core.sql_model import SQLModel
        for _class in SQLModel.registry.get_models().values():
            if not _class.__table_args__:
                continue
            for obj in _class.__table_args__:
                self.execute(obj.build_create_sql())

    def drop_all(self=None):
        from pg_orm.core.sql_model import SQLModel
        registry = SQLModel.registry
        self._drop_table_args()
        for constraint in registry.get_constraints().values():
            self.execute(constraint.build_drop_sql())
        for _class in registry.get_models().values():
            self.execute(_class.build_drop_sql())
        for _type in registry.get_types().values():
            self.execute(_type.build_drop_sql())
        return self

    def _drop_table_args(self):
        from pg_orm.core.sql_model import SQLModel
        for _class in SQLModel.registry.get_models().values():
            if not _class.__table_args__:
                continue
            for obj in _class.__table_args__:
                self.execute(obj.build_drop_sql())

    def commit(self=None) -> Self:
        self.flush()
        self._connection.commit()
        return self

    def flush(self=None) -> Self:
        for obj in self.known_objects.values():
            self._flush_obj(obj)
        for obj in self.deleted_objects.values():
            self.execute_delete(obj)
        for obj in self.created_objects:
            self._flush_obj(obj)
        return self

    def rollback(self=None):
        self._connection.rollback()
        self.expunge_all()  # TODO Should we actually clear?
        return self

    def _flush_obj(self, obj: SQLModel):
        if obj.exists_in_db:
            self._update(obj)
        else:
            self._insert(obj)

    def _update(self, obj: SQLModel) -> Self:
        if not (statement := obj.build_update(session=self)):
            return self
        self.execute(statement)
        return self

    def _insert(self, obj: SQLModel) -> Self:
        insert = obj.build_insert(session=self)
        self.execute(insert)
        obj.exists_in_db = True
        return self._replace(obj)

    def _delete(self, obj: SQLModel) -> Self:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self._cursor.__iter__()

    def __del__(self):
        self.close()


local_sessions = local()


def _get_local_session(credentials: Credentials) -> DatabaseSession | None:
    try:
        sessions: dict[str, DatabaseSession] = local_sessions.sessions
        connection_str = _connection_str_from_credentials(credentials)
        return sessions.get(connection_str)
    except AttributeError:
        return None


def _add_local_session(session: DatabaseSession):
    try:
        sessions: dict[str, DatabaseSession] = local_sessions.sessions
    except AttributeError:
        sessions = dict()
    connection_str = _connection_str_from_credentials(session.credentials)
    sessions[connection_str] = session
    setattr(local_sessions, 'sessions', sessions)


def _connection_str_from_credentials(credentials: Credentials) -> str:
    return f'{credentials.host}:{credentials.port}/{credentials.database_name}'


@atexit.register
def _cleanup_sessions():
    for sessions in DatabaseSession.__instances__.values():
        for session in sessions:
            session.close()
