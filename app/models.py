from sqlalchemy import Column, create_engine, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import String, Date, Integer
from marshmallow import Schema, fields, validate, ValidationError
from sqlalchemy.orm import relationship
import datetime

engine = create_engine('mysql+pymysql://user:secret123@db/mysql', echo=False)

Base = declarative_base()
metadata = Base.metadata


def validate_date(date_str):
    try:
        valid_date = datetime.datetime.strptime(date_str, '%d.%m.%Y')
    except ValueError:
        raise ValidationError('Invalid date.')
    if valid_date > datetime.datetime.now():
        raise ValidationError('Invalid date.')


class CitizenSchema(Schema):
    citizen_id = fields.Int(required=True, validate=validate.Range(min=1))
    town = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    street = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    building = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    apartment = fields.Int(required=True, validate=validate.Range(min=1))
    name = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    birth_date = fields.Str(required=True, validate=validate_date)
    gender = fields.Str(required=True, validate=validate.OneOf(["male", "female"]))
    relatives = fields.List(fields.Int, required=True)

class CitizenSchema_out(Schema):
    citizen_id = fields.Int(required=True, validate=validate.Range(min=1))
    town = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    street = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    building = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    apartment = fields.Int(required=True, validate=validate.Range(min=1))
    name = fields.Str(required=True, validate=validate.Length(min=1, max=256))
    birth_date = fields.Str(required=True, validate=validate_date)
    gender = fields.Str(required=True, validate=validate.OneOf(["male", "female"]))

class CitizenSchemaPatch(Schema):
    town = fields.Str(validate=validate.Length(min=1, max=256))
    street = fields.Str(validate=validate.Length(min=1, max=256))
    building = fields.Str(validate=validate.Length(min=1, max=256))
    apartment = fields.Int(validate=validate.Range(min=1))
    name = fields.Str(validate=validate.Length(min=1, max=256))
    birth_date = fields.Str(validate=validate_date)
    gender = fields.Str(validate=validate.OneOf(["male", "female"]))
    relatives = fields.List(fields.Int)


def create_citizens_collection(table_name):

    class AssociationTable(Base):
        __tablename__ = "association_table_" + table_name
        id = Column(Integer, primary_key=True)
        left_id = Column(Integer, ForeignKey(table_name + ".citizen_id"))
        right_id = Column(Integer, ForeignKey("relatives_" + table_name + ".citizen_id"))
        def __init__(self, left, right):
            self.left_id = left
            self.right_id = right

    class Relatives(Base):
        __tablename__ = "relatives_" + table_name
        citizen_id = Column(Integer, primary_key=True)

        def __init__(self, id):
            self.citizen_id = id

    class Citizen(Base):
        __tablename__ = table_name
        citizen_id = Column(Integer, primary_key=True, unique=True, nullable=False)
        town = Column(String(256), nullable=False)
        street = Column(String(256), nullable=False)
        building = Column(String(256), nullable=False)
        apartment = Column(Integer, nullable=False)
        name = Column(String(256), nullable=False)
        birth_date = Column(Date, nullable=False)
        gender = Column(String(6), nullable=False)
        rel = relationship(Relatives, secondary="association_table_" + table_name, backref="rel")

        def __init__(self, citizen):
            self.citizen_id = citizen["citizen_id"]
            self.town = citizen["town"]
            self.street = citizen["street"]
            self.building = citizen["building"]
            self.apartment = citizen["apartment"]
            self.name = citizen["name"]
            self.birth_date = datetime.datetime.strptime(citizen["birth_date"], '%d.%m.%Y')
            self.gender = citizen["gender"]

    return Citizen, Relatives, AssociationTable
