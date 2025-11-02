from enum import Enum

from pg_orm.aio.async_column import AsyncRelationship
from pg_orm.core.column import Column, EncryptedColumn, ForeignKey
from pg_orm.core.column_type import UUID, String, JSONB, ENUM
from pg_orm.core.enums import CascadeAction, IndexOption
from pg_orm.core.sql_model import ModelBase
from pg_orm.core.table_args import Index, UniqueConstraint


class TestEnum(Enum):
    ABC = 'ABC'
    DEF = 'DEF'


class Base(ModelBase):
    id = Column(UUID, primary_key=True)


class Test(Base):
    __table_name__ = 'test_one'
    label = Column(String)
    email = EncryptedColumn(String)
    flex = EncryptedColumn(JSONB)
    enum_value = Column(ENUM('enum_value', TestEnum))
    test_two_id = ForeignKey('test_two', 'id', on_delete=CascadeAction.CASCADE)

    test_two = AsyncRelationship('test_two')

    __table_args__ = (
        Index('ix_test_label', 'label', options={'label': (IndexOption.DESC, IndexOption.NULLS_LAST)}),
        UniqueConstraint('unique_email', email)
    )


class Test2(Base):
    __table_name__ = 'test_two'
    label = Column(String)
    email = EncryptedColumn(String)
    flex = EncryptedColumn(JSONB)
    # test_one_id = ForeignKey('test_one', 'id', on_delete=CascadeAction.CASCADE)
    #
    # test_one = Relationship('test_one')
