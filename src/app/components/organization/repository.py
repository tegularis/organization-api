from typing import Union
from sqlalchemy import text


class OrganizationRepository:
    def __init__(self, async_session):
        self.async_session = async_session

    async def get_by_uuid(self, uuid: str):
        """
        Select organization by uuid
        """
        query = text(
            """
                SELECT o.uuid, o.name, b.uuid AS building_uuid, b.address, b.latitude, b.longitude, 
                a.uuid AS activity_uuid, a.name AS activity_name,
                (
                    SELECT ARRAY_AGG(p.number) 
                    FROM phone_number p 
                    WHERE p.organization_id = o.id
                ) AS phone_numbers
                FROM organization o INNER JOIN building b ON o.building_id = b.id 
                INNER JOIN activity a ON o.activity_id = a.id
                WHERE o.uuid = :uuid;
            """
        )
        async with self.async_session() as session:
            res = await session.execute(query, {"uuid": uuid})
        row = res.fetchone()
        if not row:
            return
        return dict(row._mapping)

    async def get_by_name(self, name: str):
        """
        Select organization by name
        """
        query = text(
            """
                SELECT o.uuid, o.name, b.uuid AS building_uuid, b.address, b.latitude, b.longitude, 
                a.uuid AS activity_uuid, a.name AS activity_name,
                (
                    SELECT ARRAY_AGG(p.number) 
                    FROM phone_number p 
                    WHERE p.organization_id = o.id
                ) AS phone_numbers
                FROM organization o INNER JOIN building b ON o.building_id = b.id 
                INNER JOIN activity a ON o.activity_id = a.id
                WHERE o.name = :name;
            """
        )
        async with self.async_session() as session:
            res = await session.execute(query, {"name": name})
        row = res.fetchone()
        if not row:
            return
        return dict(row._mapping)

    async def get_in_radius(self, latitude: float, longitude: float, radius: float, limit: int, offset: int):
        """
        Select all organizations in given radius in meters from point with given latitude and longitude
        Uses Haversine formula
        """
        query = text(
            """
                SELECT o.uuid, o.name, b.uuid AS building_uuid, b.address, b.latitude, b.longitude, 
                a.uuid AS activity_uuid, a.name AS activity_name,
                (
                    SELECT ARRAY_AGG(p.number) 
                    FROM phone_number p 
                    WHERE p.organization_id = o.id
                ) AS phone_numbers
                FROM organization o INNER JOIN building b ON o.building_id = b.id 
                INNER JOIN activity a ON o.activity_id = a.id
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

    async def get_by_activity(self, activity: str, limit: int, offset: int):
        """
        Select all organizations with given activity name or an activity being child to given (3 levels deep)
        """
        query = text(
            """
                SELECT o.uuid, o.name, b.uuid AS building_uuid, b.address, b.latitude, b.longitude, 
                a.uuid AS activity_uuid, a.name AS activity_name,
                (
                    SELECT ARRAY_AGG(p.number) 
                    FROM phone_number p 
                    WHERE p.organization_id = o.id
                ) AS phone_numbers
                FROM organization o INNER JOIN building b ON o.building_id = b.id 
                INNER JOIN activity p_a ON o.activity_id = a.id
                LEFT JOIN activity c1_a ON c1_a.parent_id = p_a.id
                LEFT JOIN activity c2_a ON c2_a.parent_id = c1_a.id
                LEFT JOIN activity c3_a ON c3_a.parent_id = c2_a.id
                WHERE a.name = :activity OR c1_a.name = :activity OR c2_a.name = :activity OR c3_a.name = :activity     
                LIMIT :limit OFFSET :offset;
            """
        )
        async with self.async_session() as session:
            res = await session.execute(query, {"activity": activity, "limit": limit, "offset": offset})
        rows = res.fetchall()
        if not rows:
            return
        return [dict(row._mapping) for row in rows]
