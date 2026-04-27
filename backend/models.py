
import datetime

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Material(Base):
    __tablename__ = "materials"

    id = Column(Integer, primary_key=True)
    item_type = Column(String, default="equipment", nullable=False)
    name = Column(String, nullable=False)
    characteristics = Column(String)
    unit = Column(String)
    price = Column(Float, nullable=False)
    source = Column(String)
    last_update = Column(DateTime, default=datetime.datetime.utcnow)


class Work(Base):
    __tablename__ = "works"

    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey("smetas.id"), nullable=True, index=True)
    name = Column(String, nullable=False)
    characteristics = Column(String)
    unit = Column(String)
    price = Column(Float, nullable=False)
    source = Column(String)
    last_update = Column(DateTime, default=datetime.datetime.utcnow)


class Smeta(Base):
    __tablename__ = "smetas"

    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, nullable=True, index=True)
    owner_id = Column(Integer, nullable=True, index=True)
    name = Column(String, nullable=False)
    customer_name = Column(String, default="")
    customer_details = Column(String, default="")
    contractor_name = Column(String, default="")
    contractor_details = Column(String, default="")
    approver_name = Column(String, default="")
    approver_details = Column(String, default="")
    tax_mode = Column(String, default="none")
    tax_rate = Column(Float, default=0)
    section_adjustments = Column(String, default="{}")
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    items = relationship(
        "SmetaItem",
        back_populates="smeta",
        cascade="all, delete-orphan",
        order_by="SmetaItem.id",
    )


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    email = Column(String, nullable=False, unique=True, index=True)
    password_hash = Column(String, nullable=False)
    is_admin = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


class SmetaAccess(Base):
    __tablename__ = "smeta_access"

    id = Column(Integer, primary_key=True)
    smeta_id = Column(Integer, nullable=False, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    permission = Column(String, default="view", nullable=False)


class SmetaItem(Base):
    __tablename__ = "smeta_items"

    id = Column(Integer, primary_key=True)
    smeta_id = Column(Integer, ForeignKey("smetas.id"), nullable=False, index=True)
    item_type = Column(String, default="material", nullable=False)
    section = Column(String, default="Оборудование", nullable=False)
    name = Column(String, nullable=False)
    characteristics = Column(String)
    unit = Column(String)
    quantity = Column(Float, default=1, nullable=False)
    unit_price = Column(Float, nullable=False)
    base_unit_price = Column(Float, default=0, nullable=False)
    source = Column(String)

    smeta = relationship("Smeta", back_populates="items")


class SmetaRevision(Base):
    __tablename__ = "smeta_revisions"

    id = Column(Integer, primary_key=True)
    smeta_id = Column(Integer, index=True, nullable=False)
    label = Column(String, default="", nullable=False)
    payload = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
