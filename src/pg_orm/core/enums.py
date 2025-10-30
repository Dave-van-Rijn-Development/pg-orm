from enum import Enum, StrEnum


class CascadeAction(Enum):
    NO_ACTION = "NO ACTION"
    CASCADE = "CASCADE"
    RESTRICT = "RESTRICT"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"


class ModelSessionState(Enum):
    NOT_SET = "NOT_SET"
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    PERSISTED = "PERSISTED"
    DELETED = "DELETED"


class IndexOption(StrEnum):
    ASC = "ASC"
    DESC = "DESC"
    NULLS_FIRST = "NULLS FIRST"
    NULLS_LAST = "NULLS LAST"
