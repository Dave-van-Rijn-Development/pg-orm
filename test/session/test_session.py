from unittest import TestCase

from pg_orm.core.session import DatabaseSession, Credentials
from test.test_util import init_session, get_test_settings


class TestSession(TestCase):
    def test_create_session(self):
        init_session()
        settings = get_test_settings()
        self.assertEqual(Credentials.default_username, settings.database_user)
        self.assertEqual(Credentials.default_password, settings.database_password)
        self.assertEqual(Credentials.default_host, settings.database_host)
        self.assertEqual(Credentials.default_port, settings.database_port)
        self.assertEqual(Credentials.default_database_name, settings.database_name)

        session = DatabaseSession()
        self.assertEqual(session.known_objects, dict())
        self.assertEqual(session.deleted_objects, dict())
        self.assertEqual(session.created_objects, list())

    def test_creation_without_configure(self):
        """
        Test creating a session without configuring it first. This should not work unless the credentials argument is
        used
        """
        # We need to reset the configured flag to overwrite initialization of previous tests
        DatabaseSession._configured = False
        self.assertRaises(RuntimeError, DatabaseSession)
        settings = get_test_settings()

        # Creating a session with credentials and without previously configuring should work fine
        _ = DatabaseSession(credentials=Credentials(
            username=settings.database_user, password=settings.database_password, database_name=settings.database_name,
            host=settings.database_host, port=settings.database_port
        ))

    def test_auto_commit(self):
        init_session()
        session1 = DatabaseSession(auto_commit=True)
        self.assertTrue(session1.auto_commit)
        session1.auto_commit = False
        self.assertFalse(session1.auto_commit)
        session1.auto_commit = True

        # Getting the thread session with different auto_commit value should set it in the scoped session
        session2 = DatabaseSession(auto_commit=False)
        self.assertFalse(session1.auto_commit)
        session2.auto_commit = True
        self.assertTrue(session1.auto_commit)

    def test_thread_session(self):
        """
        Test that each access to the DatabaseSession in the same thread yields the same session object
        :return:
        """
        init_session()
        session1 = DatabaseSession()
        session2 = DatabaseSession()
        # Forcing the connection should create a new session instead of yielding an existing session
        session3 = DatabaseSession(isolate=True)
        self.assertEqual(session1, session2)
        self.assertEqual(id(session1), id(session2))
        self.assertNotEqual(session1, session3)

        # Access _connection to force establishing the actual database connection
        _ = session1._connection

        # Closing one session should also close the other session
        self.assertFalse(session2.connection_closed)
        session1.close()
        self.assertTrue(session2.connection_closed)
