from functools import partial
import operator

from huey.api import Huey
from huey.constants import EmptyData
from huey.exceptions import ConfigurationError
from huey.storage import BaseStorage

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.types import BigInteger, DateTime, Float, LargeBinary, Text

metadata = MetaData()
KV = Table('kvs', metadata,
    Column('queue', Text, primary_key=True),
    Column('key', Text, primary_key=True),
    Column('value', LargeBinary, nullable=False),
)

Schedule = Table('schedules', metadata,
    Column('id', BigInteger, nullable=False, primary_key=True),
    Column('queue', Text, nullable=False),
    Column('data', LargeBinary, nullable=False),
    Column('timestamp', DateTime(timezone=True), nullable=False),
    Index('queue_timestamp_idx', 'queue', 'timestamp'),
)

Task = Table('tasks', metadata,
    Column('id', BigInteger, nullable=False, primary_key=True),
    Column('queue', Text, nullable=False),
    Column('data', LargeBinary, nullable=False),
    Column('priority', Float, nullable=False, default=0.0),
    Index('priority_idx', 'priority', 'id'),
)


class SQLAlchemyStorage(BaseStorage):
    def __init__(self, name='huey', engine=None, **kwargs):
        super(SQLAlchemyStorage, self).__init__(name)

        if engine is None:
            raise ConfigurationError('Use of SQLAlchemyStorage requires an '
                                     'engine= argument, which should be a '
                                     'SQLAlchemy engine or a connection string.')

        if isinstance(engine, Engine):
            self.engine = engine
        else:
            # Treat database argument as a URL connection string.
            self.engine = create_engine(engine)

        metadata.create_all(bind=self.engine)

    def drop_tables(self):
        metadata.drop_all(bind=self.engine)

    def close(self):
        return self.database.close()

    def tasks(self, *columns):
        return self.Task.select(*columns).where(self.Task.queue == self.name)

    def schedule(self, *columns):
        return (self.Schedule.select(*columns)
                .where(self.Schedule.queue == self.name))

    def kv(self, *columns):
        return self.KV.select(*columns).where(self.KV.queue == self.name)

    def enqueue(self, data, priority=None):
        self.Task.create(queue=self.name, data=data, priority=priority or 0)

    def dequeue(self):
        query = (self.tasks(self.Task.id, self.Task.data)
                 .order_by(self.Task.priority.desc(), self.Task.id)
                 .limit(1))
        if self.database.for_update:
            query = query.for_update()

        with self.database.atomic():
            try:
                task = query.get()
            except self.Task.DoesNotExist:
                return

            nrows = self.Task.delete().where(self.Task.id == task.id).execute()
            if nrows == 1:
                return task.data

    def queue_size(self):
        return self.tasks().count()

    def enqueued_items(self, limit=None):
        query = self.tasks(self.Task.data).order_by(self.Task.priority.desc(),
                                                    self.Task.id)
        if limit is not None:
            query = query.limit(limit)
        return list(map(operator.itemgetter(0), query.tuples()))

    def flush_queue(self):
        self.Task.delete().where(self.Task.queue == self.name).execute()

    def add_to_schedule(self, data, timestamp, utc):
        self.Schedule.create(queue=self.name, data=data, timestamp=timestamp)

    def read_schedule(self, timestamp):
        query = (self.schedule(self.Schedule.id, self.Schedule.data)
                 .where(self.Schedule.timestamp <= timestamp)
                 .tuples())
        if self.database.for_update:
            query = query.for_update()

        with self.database.atomic():
            results = list(query)
            if not results:
                return []

            id_list, data = zip(*results)
            (self.Schedule
             .delete()
             .where(self.Schedule.id.in_(id_list))
             .execute())

            return list(data)

    def schedule_size(self):
        return self.schedule().count()

    def scheduled_items(self):
        tasks = (self.schedule(self.Schedule.data)
                 .order_by(self.Schedule.timestamp)
                 .tuples())
        return list(map(operator.itemgetter(0), tasks))

    def flush_schedule(self):
        (self.Schedule
         .delete()
         .where(self.Schedule.queue == self.name)
         .execute())

    def put_data(self, key, value, is_result=False):
        if isinstance(self.database, PostgresqlDatabase):
            (self.KV
             .insert(queue=self.name, key=key, value=value)
             .on_conflict(conflict_target=[self.KV.queue, self.KV.key],
                          preserve=[self.KV.value])
             .execute())
        else:
            self.KV.replace(queue=self.name, key=key, value=value).execute()

    def peek_data(self, key):
        try:
            kv = self.kv(self.KV.value).where(self.KV.key == key).get()
        except self.KV.DoesNotExist:
            return EmptyData
        else:
            return kv.value

    def pop_data(self, key):
        query = self.kv().where(self.KV.key == key)
        if self.database.for_update:
            query = query.for_update()

        with self.database.atomic():
            try:
                kv = query.get()
            except self.KV.DoesNotExist:
                return EmptyData
            else:
                dq = self.KV.delete().where(
                    (self.KV.queue == self.name) &
                    (self.KV.key == key))
                return kv.value if dq.execute() == 1 else EmptyData

    def has_data_for_key(self, key):
        return self.kv().where(self.KV.key == key).exists()

    def put_if_empty(self, key, value):
        try:
            with self.database.atomic():
                self.KV.insert(queue=self.name, key=key, value=value).execute()
        except IntegrityError:
            return False
        else:
            return True

    def result_store_size(self):
        return self.kv().count()

    def result_items(self):
        query = self.kv(self.KV.key, self.KV.value).tuples()
        return dict((k, v) for k, v in query.iterator())

    def flush_results(self):
        self.KV.delete().where(self.KV.queue == self.name).execute()


SQLAlchemyHuey = partial(Huey, storage_class=SQLAlchemyStorage)

