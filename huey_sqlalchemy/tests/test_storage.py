import contextlib
import datetime
import logging
import unittest

from huey.constants import EmptyData
from huey.consumer import Consumer
from huey.exceptions import TaskException

from huey_sqlalchemy import SQLAlchemyHuey


class NullHandler(logging.Handler):
    def emit(self, record):
        pass


logger = logging.getLogger("huey")


class BaseTestCase(unittest.TestCase):
    consumer_class = Consumer

    def setUp(self):
        super(BaseTestCase, self).setUp()
        self.huey = self.get_huey()

    def execute_next(self):
        task = self.huey.dequeue()
        self.assertTrue(task is not None)
        return self.huey.execute(task)

    def trap_exception(self, fn, exc_type=TaskException):
        try:
            fn()
        except exc_type as exc_val:
            return exc_val
        raise AssertionError("trap_exception() failed to catch %s" % exc_type)

    def consumer(self, **params):
        params.setdefault("initial_delay", 0.001)
        params.setdefault("max_delay", 0.001)
        params.setdefault("workers", 2)
        params.setdefault("check_worker_health", False)
        return self.consumer_class(self.huey, **params)

    @contextlib.contextmanager
    def consumer_context(self, **kwargs):
        consumer = self.consumer(**kwargs)
        consumer.start()
        try:
            yield
        finally:
            consumer.stop(graceful=True)


class StorageTests(object):
    destructive_reads = True

    def setUp(self):
        super(StorageTests, self).setUp()
        self.s = self.huey.storage
        self.s.flush_all()

    def tearDown(self):
        super(StorageTests, self).tearDown()
        self.s.flush_all()

    def test_queue_methods(self):
        for i in range(3):
            self.s.enqueue(b"item-%d" % i)

        # Remove two items (this API is not used, but we'll test it anyways).
        self.assertEqual(self.s.dequeue(), b"item-0")
        self.assertEqual(self.s.queue_size(), 2)
        self.assertEqual(self.s.enqueued_items(), [b"item-1", b"item-2"])
        self.assertEqual(self.s.dequeue(), b"item-1")
        self.assertEqual(self.s.dequeue(), b"item-2")
        self.assertTrue(self.s.dequeue() is None)

        self.assertEqual(self.s.queue_size(), 0)

        # Test flushing the queue.
        self.s.enqueue(b"item-3")
        self.assertEqual(self.s.queue_size(), 1)
        self.s.flush_queue()
        self.assertEqual(self.s.queue_size(), 0)

    def test_schedule_methods(self):
        timestamp = datetime.datetime(2000, 1, 2, 3, 4, 5)
        second = datetime.timedelta(seconds=1)

        items = (
            (b"p1", timestamp + second),
            (b"p0", timestamp),
            (b"n1", timestamp - second),
            (b"p2", timestamp + second + second),
        )
        for data, ts in items:
            self.s.add_to_schedule(data, ts, False)

        self.assertEqual(self.s.schedule_size(), 4)

        # Read from the schedule up-to the "p0" timestamp.
        sched = self.s.read_schedule(timestamp)
        self.assertEqual(sched, [b"n1", b"p0"])

        self.assertEqual(self.s.scheduled_items(), [b"p1", b"p2"])
        self.assertEqual(self.s.schedule_size(), 2)
        sched = self.s.read_schedule(datetime.datetime.now())
        self.assertEqual(sched, [b"p1", b"p2"])
        self.assertEqual(self.s.schedule_size(), 0)
        self.assertEqual(self.s.read_schedule(datetime.datetime.now()), [])

    def test_result_store_methods(self):
        # Put and peek at data. Verify missing keys return EmptyData sentinel.
        self.s.put_data(b"k1", b"v1")
        self.s.put_data(b"k2", b"v2")
        self.assertEqual(self.s.peek_data(b"k2"), b"v2")
        self.assertEqual(self.s.peek_data(b"k1"), b"v1")
        self.assertTrue(self.s.peek_data(b"kx") is EmptyData)
        self.assertEqual(self.s.result_store_size(), 2)

        # Verify we can overwrite existing keys and that pop will remove the
        # key/value pair. Subsequent pop on missing key will return EmptyData.
        self.s.put_data(b"k1", b"v1-x")
        self.assertEqual(self.s.peek_data(b"k1"), b"v1-x")
        self.assertEqual(self.s.pop_data(b"k1"), b"v1-x")
        if self.destructive_reads:
            self.assertTrue(self.s.pop_data(b"k1") is EmptyData)
        else:
            self.assertEqual(self.s.pop_data(b"k1"), b"v1-x")
            self.assertTrue(self.s.delete_data(b"k1"))

        self.assertFalse(self.s.has_data_for_key(b"k1"))
        self.assertTrue(self.s.has_data_for_key(b"k2"))
        self.assertEqual(self.s.result_store_size(), 1)

        # Test put-if-empty.
        self.assertTrue(self.s.put_if_empty(b"k1", b"v1-y"))
        self.assertFalse(self.s.put_if_empty(b"k1", b"v1-z"))
        self.assertEqual(self.s.peek_data(b"k1"), b"v1-y")

        # Test deletion.
        self.assertTrue(self.s.put_if_empty(b"k3", b"v3"))
        self.assertTrue(self.s.delete_data(b"k3"))
        self.assertFalse(self.s.delete_data(b"k3"))

        # Test introspection.
        state = self.s.result_items()  # Normalize keys to unicode strings.
        clean = {
            k.decode("utf8") if isinstance(k, bytes) else k: v for k, v in state.items()
        }
        self.assertEqual(clean, {"k1": b"v1-y", "k2": b"v2"})
        self.s.flush_results()
        self.assertEqual(self.s.result_store_size(), 0)
        self.assertEqual(self.s.result_items(), {})

    def test_priority(self):
        if not self.s.priority:
            raise unittest.SkipTest("priority support required")

        priorities = (1, None, 5, None, 3, None, 9, None, 7, 0)
        for i, priority in enumerate(priorities):
            item = "i%s-%s" % (i, priority)
            self.s.enqueue(item.encode("utf8"), priority)

        expected = [
            b"i6-9",
            b"i8-7",
            b"i2-5",
            b"i4-3",
            b"i0-1",
            b"i1-None",
            b"i3-None",
            b"i5-None",
            b"i7-None",
            b"i9-0",
        ]
        self.assertEqual([self.s.dequeue() for _ in range(10)], expected)

    def test_consumer_integration(self):
        @self.huey.task()
        def task_a(n):
            return n + 1

        with self.consumer_context():
            r1 = task_a(1)
            r2 = task_a(2)
            r3 = task_a(3)

            self.assertEqual(r1.get(blocking=True, timeout=5), 2)
            self.assertEqual(r2.get(blocking=True, timeout=5), 3)
            self.assertEqual(r3.get(blocking=True, timeout=5), 4)

            task_a.revoke()
            self.assertTrue(task_a.is_revoked())
            self.assertTrue(task_a.restore())


class TestSQLAlchemyStorage(StorageTests, BaseTestCase):
    def setUp(self):
        super().setUp()
        self.huey.storage.create_tables()

    def tearDown(self):
        super().tearDown()
        self.huey.storage.drop_tables()

    def get_huey(self):
        return SQLAlchemyHuey(engine="postgresql://tests:tests@localhost:5432")
