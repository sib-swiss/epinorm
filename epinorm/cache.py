import sqlite3

from epinorm.config import (
    SCRIPT_DIR,
    WORK_DIR,
)

SCHEMA_FILE = SCRIPT_DIR / "cache_db_schema.sql"
DB_FILE = WORK_DIR / "cache.db"


class Cache:
    pass


class SQLiteCache(Cache):

    def __init__(self):
        super().__init__()
        print(DB_FILE)
        self._connection = sqlite3.connect(DB_FILE)
        self._initialize_database()
        self._enforce_foreign_keys()

    def _get_cursor(self):
        """Return a cursor object."""
        return self._connection.cursor()

    def _commit_transaction(self):
        """Commit the current transaction."""
        self._connection.commit()

    def _close_connection(self):
        """Close the database connection."""
        self._connection.close()

    def _initialize_database(self):
        """Initialize the database with the schema defined in the schema file."""
        cursor = self._get_cursor()
        # Check if the database is empty
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        # Execute database schema creation script
        if not tables:
            with open(SCHEMA_FILE, "r") as file:
                cursor.executescript(file.read())
        cursor.close()
        self._commit_transaction()

    def _enforce_foreign_keys(self):
        """Enforce foreign key constraints."""
        cursor = self._get_cursor()
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.close()
        self._commit_transaction()

    def _get_record(self, cursor):
        """Return a dictionary representation of the current row in the cursor."""
        record = cursor.fetchone()
        if record:
            columns = [column[0] for column in cursor.description]
            record = dict(zip(columns, record))
        return record if record else None

    def _get_records(self, cursor):
        """Return a list of dictionary representations of the rows in the cursor."""
        records = cursor.fetchall()
        if records:
            columns = [column[0] for column in cursor.description]
            records = [dict(zip(columns, record)) for record in records]
        return records if records else None

    def get_feature(self, feature_id):
        """Get a feature from the cache database."""
        cursor = self._get_cursor()
        cursor.execute(
            "SELECT * FROM feature WHERE id = ?",
            (feature_id,),
        )
        feature = self._get_record(cursor)
        cursor.close()
        return feature

    def get_features(self, feature_ids):
        """Get multiple features from the cache database."""
        cursor = self._get_cursor()
        cursor.execute("CREATE TEMPORARY TABLE selected_feature (id TEXT)")
        cursor.executemany(
            "INSERT INTO selected_feature (id) VALUES (?)",
            [(id,) for id in feature_ids],
        )
        cursor.execute(
            """
                SELECT *
                FROM feature
                    INNER JOIN selected_feature
                        ON feature.id = selected_feature.id
            """
        )
        features = self._get_records(cursor)
        cursor.execute("DROP TABLE selected_feature")
        cursor.close()
        return features

    def find_feature(self, term):
        """Find a feature in the cache database."""
        cursor = self._get_cursor()
        cursor.execute(
            """
            SELECT feature.*
            FROM feature_index
                INNER JOIN feature ON feature_index.feature_id = feature.id
            WHERE term = ?
            """,
            (term,),
        )
        feature = self._get_record(cursor)
        cursor.close()
        return feature

    def save_feature(self, feature, term=None, term_type=None):
        """Save a feature to the cache database."""
        cursor = self._get_cursor()
        statement = """
            INSERT OR IGNORE INTO feature (
                id,
                osm_id,
                osm_type,
                name,
                address,
                place_rank,
                latitude,
                longitude,
                bounding_box,
                polygon
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(statement, tuple(feature.values()))
        if term and term_type:
            statement = """
                INSERT OR IGNORE INTO feature_index (
                    term,
                    term_type,
                    feature_id
                ) VALUES (?, ?, ?)
            """
            cursor.execute(statement, (term, term_type, feature.get("id")))
        cursor.close()
        self._commit_transaction()

    def delete_feature(self, feature_id):
        """Delete a feature from the cache database."""
        cursor = self._get_cursor()
        cursor.execute(
            "DELETE FROM feature WHERE id = ?",
            (feature_id,),
        )
        cursor.close()
        self._commit_transaction()

    def delete_all(self):
        """Clear the cache database."""
        cursor = self._get_cursor()
        cursor.execute("DELETE FROM feature")
        cursor.execute("DELETE FROM feature_index")
        cursor.close()
        self._commit_transaction()

    @staticmethod
    def delete_db():
        """Delete the cache database."""
        if DB_FILE.exists():
            DB_FILE.unlink()
