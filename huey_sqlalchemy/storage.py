import operator
from typing import Dict, List, Optional, Union, cast, Any
from typing_extensions import Literal
from datetime import datetime

from huey.constants import EmptyData
from huey.exceptions import ConfigurationError
from huey.storage import BaseStorage

from sqlalchemy import create_engine, delete, select, update, func
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData, Table, Column, Index
from sqlalchemy.sql.expression import Select, ClauseElement, literal, Executable
from sqlalchemy.types import BigInteger, DateTime, Float, LargeBinary, Text


def ensure_bytes(value: Union[str, bytes]) -> bytes:
    if isinstance(value, str):
        return value.encode()
    return value


metadata = MetaData()
KV = Table(
    "kvs",
    metadata,
    Column("queue", Text, primary_key=True),
    Column("key", LargeBinary, primary_key=True),
    Column("value", LargeBinary, nullable=False),
)

Schedule = Table(
    "schedules",
    metadata,
    Column("id", BigInteger, nullable=False, primary_key=True),
    Column("queue", Text, nullable=False),
    Column("data", LargeBinary, nullable=False),
    Column("timestamp", DateTime(timezone=True), nullable=False),
    Index("queue_timestamp_idx", "queue", "timestamp"),
)

Task = Table(
    "tasks",
    metadata,
    Column("id", BigInteger, nullable=False, primary_key=True),
    Column("queue", Text, nullable=False),
    Column("data", LargeBinary, nullable=False),
    Column("priority", Float, nullable=False, default=0.0),
    Index("priority_idx", "priority", "id"),
)


class SQLAlchemyStorage(BaseStorage):
    engine: Engine

    def __init__(
        self,
        name: str = "huey",
        engine: Union[Engine, str, None] = None,
        **kwargs: Dict
    ) -> None:
        super().__init__(name)

        if engine is None:
            raise ConfigurationError(
                "Use of SQLAlchemyStorage requires an "
                "engine= argument, which should be a "
                "SQLAlchemy engine or a connection string."
            )

        if isinstance(engine, Engine):
            self.engine = engine
        else:
            # Treat database argument as a URL connection string.
            self.engine = create_engine(engine)

        self.create_tables()

    def create_tables(self) -> None:
        metadata.create_all(self.engine)

    def drop_tables(self) -> None:
        metadata.drop_all(bind=self.engine)

    def close(self) -> None:
        self.engine.dispose()

    def tasks(self, op: Any = select, *columns: ClauseElement) -> Any:
        return op(columns or Task).where(Task.c.queue == self.name)

    def schedules(self, op: Any = select, *columns: ClauseElement) -> Any:
        return op(columns or Schedule).where(Schedule.c.queue == self.name)

    def kvs(self, op: Any = select, *columns: ClauseElement) -> Any:
        return op(columns or KV).where(KV.c.queue == self.name)

    def enqueue(self, data: bytes, priority: Optional[float] = None) -> int:
        extra_data = {}
        if priority is not None:
            extra_data["priority"] = priority
        return self.engine.execute(
            Task.insert()
            .values(queue=self.name, data=data, **extra_data)
            .returning(Task.c.id)
        ).scalar()

    def dequeue(self) -> Optional[bytes]:
        query = (
            self.tasks(select, Task.c.id, Task.c.data)
            .with_for_update(skip_locked=True)
            .order_by(Task.c.priority.desc(), Task.c.id)
            .limit(1)
        )

        with self.engine.begin() as conn:
            task = conn.execute(query).first()
            if task is None:
                return None
            conn.execute(delete(Task).where(Task.c.id == task.id))
            return task.data

    def queue_size(self) -> int:
        return self.engine.execute(self.tasks(select, func.count("*"))).scalar()

    def enqueued_items(self, limit: Optional[int] = None) -> List[bytes]:
        query = self.tasks(select, Task.c.data).order_by(
            Task.c.priority.desc(), Task.c.id
        )
        if limit is not None:
            query = query.limit(limit)
        rows = self.engine.execute(query)
        return [r.data for r in rows]

    def flush_queue(self) -> None:
        self.engine.execute(self.tasks(delete))

    def add_to_schedule(self, data: bytes, timestamp: datetime, utc: bool) -> None:
        self.engine.execute(
            Schedule.insert().values(queue=self.name, data=data, timestamp=timestamp)
        )

    def read_schedule(self, timestamp: datetime) -> List[bytes]:
        query = (
            self.schedules(select, Schedule.c.id, Schedule.c.data)
            .with_for_update()
            .where(Schedule.c.timestamp <= timestamp)
        )

        with self.engine.begin() as conn:
            rows = conn.execute(query).fetchall()
            if not rows:
                return []

            ids, data = zip(*((r.id, r.data) for r in rows))
            conn.execute(Schedule.delete().where(Schedule.c.id.in_(ids)))

            return list(data)

    def schedule_size(self) -> int:
        return self.engine.execute(self.schedules(select, func.count("*"))).scalar()

    def scheduled_items(self, limit: Optional[int] = None) -> List[bytes]:
        query = self.schedules(select, Schedule.c.data).order_by(Schedule.c.timestamp)
        if limit is not None:
            query = query.limit(limit)
        rows = self.engine.execute(query)
        return [r.data for r in rows]

    def flush_schedule(self) -> None:
        self.engine.execute(self.schedules(delete))

    def put_data(self, key: bytes, value: bytes, is_result: bool = False) -> None:
        key = ensure_bytes(key)
        if self.engine.name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert

            insert_stmt = pg_insert(KV).values(queue=self.name, key=key, value=value)
            self.engine.execute(
                insert_stmt.on_conflict_do_update(
                    index_elements=[KV.c.queue, KV.c.key],
                    set_={"value": insert_stmt.excluded.value},
                )
            )
        elif self.engine.name == "mysql":
            from sqlalchemy.dialects.mysql import insert as mysql_insert

            insert_stmt = mysql_insert(KV).values(queue=self.name, key=key, value=value)
            self.engine.execute(
                insert_stmt.on_conflict_do_update(value=insert_stmt.inserted.value)
            )
        else:
            with self.engine.begin() as conn:
                exists = conn.execute(
                    self.kvs(select, literal(True)).where(KV.c.key == key)
                ).scalar()
                if exists is None:
                    query = KV.insert().values(queue=self.name, key=key, value=value)
                else:
                    query = self.kvs(update).where(KV.c.key == key).values(value=value)
                conn.execute(query)

    def peek_data(self, key: bytes) -> bytes:
        key = ensure_bytes(key)
        value = self.engine.execute(
            self.kvs(select, KV.c.value).where(KV.c.key == key)
        ).scalar()
        if value is None:
            return EmptyData
        return value

    def pop_data(self, key: bytes) -> bytes:
        key = ensure_bytes(key)
        value = self.engine.execute(
            self.kvs(delete).where(KV.c.key == key).returning(KV.c.value)
        ).scalar()
        if value is None:
            return EmptyData
        return value

    def has_data_for_key(self, key: bytes) -> bool:
        key = ensure_bytes(key)
        return (
            self.engine.execute(
                self.kvs(select, literal(True)).where(KV.c.key == key)
            ).scalar()
            is not None
        )

    def put_if_empty(self, key: bytes, value: bytes) -> bool:
        key = ensure_bytes(key)
        try:
            self.engine.execute(
                KV.insert().values(queue=self.name, key=key, value=value)
            )
        except IntegrityError as e:
            return False
        else:
            return True

    def result_store_size(self) -> int:
        return self.engine.execute(self.kvs(select, func.count("*"))).scalar()

    def result_items(self) -> Dict[str, bytes]:
        kv_rows = self.engine.execute(self.kvs(select, KV.c.key, KV.c.value))
        return dict((r.key, r.value) for r in kv_rows)

    def flush_results(self) -> None:
        self.engine.execute(self.kvs(delete))
