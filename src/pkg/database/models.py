import uuid
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import (Column, Integer, String, func, ForeignKey, UUID, select, update, Float, delete, and_,
                        CheckConstraint)
from config.main import Config
from src.pkg.hasher.main import Hasher

hasher = Hasher()

cfg = Config("config/config.yml").load()

url = (f"postgresql+asyncpg://{cfg['database']['user']}:{cfg['database']['password']}@{cfg['database']['host']}:"
       f"{cfg['database']['port']}/{cfg['database']['name']}")

engine = create_async_engine(url,
                             pool_size=100,
                             max_overflow=50)

async_session = async_sessionmaker(engine, expire_on_commit=False)


class Base(declarative_base()):
    __abstract__ = True
    async_session = async_session

    async def save(self):
        async with self.async_session() as session:
            session.add(self)
            await session.commit()
            await session.refresh(self)
            return self

    @classmethod
    async def delete(cls, **kwargs):
        query = delete(cls).where(
            and_(*[getattr(cls, key) == value for key, value in kwargs.items()]))
        async with cls.async_session() as session:
            await session.execute(query)
            await session.commit()

    @classmethod
    async def get(cls, **kwargs):
        query = select(cls).filter_by(**kwargs)
        async with cls.async_session() as session:
            result = await session.execute(query)
        return result.scalar()

    @classmethod
    async def get_all(cls, **kwargs):
        order = kwargs.pop('order', None)
        limit = kwargs.pop('limit', None)
        offset = kwargs.pop('offset', None)
        query = select(cls).filter_by(**kwargs).order_by(order).limit(limit).offset(offset)
        async with cls.async_session() as session:
            result = await session.execute(query)
        return result.scalars().all()

    @classmethod
    async def count(cls, **kwargs):
        query = select(func.count()).select_from(cls).filter_by(**kwargs)
        async with cls.async_session() as session:
            result = await session.execute(query)
        return result.scalar()

    @classmethod
    async def update(cls, fields: dict, **kwargs):
        async with cls.async_session() as session:
            query = update(cls)
            conditions = []
            for key, value in kwargs.items():
                column = getattr(cls, key, None)
                if column is None:
                    raise ValueError(f"Invalid column name: {key}")
                conditions.append(column == value)
            if conditions:
                query = query.where(*conditions)
            query = query.values(fields)
            res = await session.execute(query)
            await session.commit()
            return res.rowcount


class ApiKey(Base):
    __tablename__ = 'api_key'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    hashed_key = Column(String, nullable=False)

    def __init__(self, key):
        self.hashed_key = hasher.get_hash(key)


class Building(Base):
    __tablename__ = 'building'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    uuid = Column(UUID, default=uuid.uuid4)
    address = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    __table_args__ = (
        CheckConstraint('latitude >= -90 AND latitude <= 90', name='check_latitude'),
        CheckConstraint('longitude >= -180 AND longitude <= 180', name='check_longitude'),
    )

    def __init__(self, address, latitude, longitude):
        self.address = address
        self.latitude = latitude
        self.longitude = longitude


class Activity(Base):
    __tablename__ = 'activity'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    uuid = Column(UUID, default=uuid.uuid4)
    name = Column(String, nullable=False)
    parent_id = Column(Integer, ForeignKey('activity.id'), nullable=True, index=True)

    def __init__(self, name, parent_id=None):
        self.name = name
        self.parent_id = parent_id


class Organization(Base):
    __tablename__ = 'organization'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    uuid = Column(UUID, default=uuid.uuid4)
    name = Column(String, nullable=False)
    building_id = Column(Integer, ForeignKey(Building.id), nullable=False)
    activity_id = Column(Integer, ForeignKey(Activity.id), nullable=False)

    def __init__(self, name, building_id, activity_id):
        self.name = name
        self.building_id = building_id
        self.activity_id = activity_id


class PhoneNumber(Base):
    __tablename__ = 'phone_number'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    number = Column(String, nullable=False)
    organization_id = Column(Integer, ForeignKey(Organization.id), nullable=False)

    def __init__(self, number, organization_id):
        self.number = number
        self.organization_id = organization_id


async def insert_data():
    key = "A5z~V2g+T8f*D0m^L!1"
    await ApiKey(key=key).save()

    building = await Building.get(address="Пятницкая улица, 10 ст1", latitude=55.743748, longitude=37.62795)
    if not building:
        building = await Building(address="Пятницкая улица, 10 ст1", latitude=55.743748, longitude=37.62795).save()

    store = await Activity.get(name="Shop")
    if not store:
        store = await Activity(name="Shop").save()

    flowers_store = await Activity.get(name="Flowers shop", parent_id=store.id)
    if not flowers_store:
        flowers_store = await Activity(name="Flowers shop", parent_id=store.id).save()

    organization = await Organization.get(
        name="Rose & Tulip", building_id=building.id, activity_id=flowers_store.id)
    if not organization:
        organization = await Organization(
            name="Rose & Tulip", building_id=building.id, activity_id=flowers_store.id).save()

    if not await PhoneNumber.get(number="+7‒925‒645‒XX‒XX", organization_id=organization.id):
        await PhoneNumber(number="+7‒925‒645‒XX‒XX", organization_id=organization.id).save()

    if not await PhoneNumber.get(number="+7‒915‒765‒XX‒XX", organization_id=organization.id):
        await PhoneNumber(number="+7‒915‒765‒XX‒XX", organization_id=organization.id).save()


async def create_models(insert_test_data: bool=False):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    if insert_test_data:
        await insert_data()
