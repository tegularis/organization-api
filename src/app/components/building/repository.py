from typing import Union
from sqlalchemy import text


class BuildingRepository:
    def __init__(self, async_session):
        self.async_session = async_session

    async def get_by_uuid(self, uuid: str):
        """
        Select building by uuid
        """
        query = text(
            """
                SELECT b.uuid, b.address, b.latitude, b.longitude
                FROM building b
                WHERE b.uuid = :uuid;
            """
        )
        async with self.async_session() as session:
            res = await session.execute(query, {"uuid": uuid})
        row = res.fetchone()
        if not row:
            return
        return dict(row._mapping)

    async def get_in_radius(self, latitude: float, longitude: float, radius: float, limit: int, offset: int):
        """
        Select all buildings in given radius in meters from point with given latitude and longitude
        Uses Haversine formula
        """
        query = text(
            """
                SELECT b.uuid, b.address, b.latitude, b.longitude
                FROM building b
                WHERE (
                    6378000 * acos(
                        cos(radians(:latitude)) * cos(radians(b.latitude)) *
                        cos(radians(b.longitude) - radians(:longitude)) +
                        sin(radians(:latitude)) * sin(radians(b.latitude))
                    )
                ) <= :radius
                LIMIT :limit OFFSET :offset;
            """
        )
        async with self.async_session() as session:
            res = await session.execute(
                query, {"latitude": latitude, "longitude": longitude, "radius": radius, "limit": limit,
                        "offset": offset})
        rows = res.fetchall()
        if not rows:
            return
        return [dict(row._mapping) for row in rows]
