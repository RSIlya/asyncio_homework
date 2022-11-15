import asyncio

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

ASYNC_DSN = 'postgresql+asyncpg://user:1234@localhost:8000/swapi_db'
# ASYNC_DSN = 'sqlite+aiosqlite:///swapi.db'

engine = create_async_engine(ASYNC_DSN, echo=False)
Base = declarative_base()
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Character(Base):
    __tablename__ = "character"

    id = Column(Integer, primary_key=True, autoincrement=False)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String) #строка с названиями фильмов через запятую
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String) #строка с названиями типов через запятую
    starships = Column(String) #строка с названиями кораблей через запятую
    vehicles = Column(String) #строка с названиями транспорта через запятую


async def migrate():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

async def transfer_to_db(data: list):
    async with async_session() as session:
        async with session.begin():
            for person in data:
                if person['status'] == 200:
                    person = Character(
                        id=person['id'],
                        birth_year=person['birth_year'],
                        eye_color=person['eye_color'],
                        films=", ".join(person['films']),
                        gender=person['gender'],
                        hair_color=person['hair_color'],
                        height=person['height'],
                        homeworld=person['homeworld'],
                        mass=person['mass'],
                        name=person['name'],
                        skin_color=person['skin_color'],
                        species=", ".join(person['species']),
                        starships=", ".join(person['starships']),
                        vehicles=", ".join(person['vehicles']),
                    )
                    session.add(person)


if __name__=="__main__":
    asyncio.run(migrate())
