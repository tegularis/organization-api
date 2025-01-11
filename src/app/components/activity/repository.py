from typing import Union
from sqlalchemy import text


class ActivityRepository:
    def __init__(self, async_session):
        self.async_session = async_session

    async def get_by_uuid(self, uuid: str):
        """
        Select activity by uuid
        """
        query = text(
            """
                SELECT a.uuid, a.name, p.uuid AS parent_uuid, p.name AS parent_name
                FROM activity a LEFT JOIN activity p ON a.parent_id = p.id
                WHERE a.uuid = :uuid;
            """
        )
        async with self.async_session() as session:
            res = await session.execute(query, {"uuid": uuid})
        row = res.fetchone()
        if not row:
            return
        return dict(row._mapping)

    async def get_all(self, limit: int, offset: int):
        """
        Select all activities
        """
        query = text(
            """
                SELECT a.uuid, a.name, p.uuid AS parent_uuid, p.name AS parent_name
                FROM activity a LEFT JOIN activity p ON a.parent_id = p.id
                LIMIT :limit OFFSET :offset;
            """
        )
        async with self.async_session() as session:
            res = await session.execute(query, {"limit": limit, "offset": offset})
        rows = res.fetchall()
        if not rows:
            return []
        return [dict(row._mapping) for row in rows]
