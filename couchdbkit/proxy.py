from couchdbkit.client import Database
from couchdbkit.schema.base import Document

class DBProxy(object):
    """A database proxy. Allows for performing the most common operations on a
    document in different databases

    >>> from couchdbkit import Database, DBProxy
    >>> server = Server()
    >>> db1 = server.create_db('couchdbkit_test')
    >>> db2 = server.create_db('couchdbkit_test2')
    >>> proxy = DBProxy(customer_1=db1)
    >>> 'customer_1' in proxy
    True
    >>> 'customer_2' in proxy
    False
    >>> proxy.add_db('customer2', db2)
    >>> 'customer_2' in proxy
    True
    >>> server.delete_db('couchdbkit_test')
    >>> server.delete_db('couchdbkit_test2')
    """
    def __init__(self, **entries):
        self._entries = entries

    def __contains__(self, dbname):
        return dbname in self._entries

    def add_db(self, dbname, db):
        """Dynamically add another db to the proxy"""
        if not isinstance(db, Database):
            raise TypeError("Db must be a database")
        self._entries[dbname] = db

    def delete_db(self, dbname):
        """Deletes the database name from the proxy."""
        del self._entries[dbname]

    def delete_attachment(self, dbname, doc, name):
        """Deletes an attachment with a given name"""
        db = self._entries[dbname]
        result = db.delete_attachment(doc._doc, name)
        try:
            doc['_attachments'].pop(name)
        except KeyError:
            pass
        return result

    def fetch_attachment(self, dbname, doc, name, stream=False):
        db = self._entries[dbname]
        return db.fetch_attachment(doc._doc, name, stream=stream)

    def put_attachment(self, dbname, doc, content, name=None,
                       content_type=None, content_length=None):
        db = self._entries[dbname]
        return db.put_attachment(doc._doc, content, name=name,
                                 content_type=content_type,
                                 content_length=content_length)

    def get_db(self, dbname):
        """Returns a database with a particular name"""
        return self._entries[dbname]

    def get(self, dbname, cls, docid, rev=None, dynamic_properties=True):
        """Returns a document with a particular ID"""
        db = self._entries[dbname]
        cls._allow_dynamic_properties = dynamic_properties
        return db.get(docid, rev=rev, wrapper=cls.wrap)

    def delete(self, dbname, doc):
        db = self._entries[dbname]
        if doc.new_document:
            raise TypeError("the document is not saved")

        db.delete_doc(doc._id)

        # reinit document
        del doc._doc['_id']
        del doc._doc['_rev']

    def view(self, dbname, cls, view, wrap_doc=True, dynamic_properties=True,
             **params):
        def default_wrapper(row):
            data = row.get('value')
            docid = row.get('id')
            doc = row.get('doc')
            if doc is not None and wrap_doc:
                cls._allow_dynamic_properties = dynamic_properties
                return cls.wrap(doc)
            elif not data or data is None:
                return row
            elif not isinstance(data, dict) or not docid:
                return row
            else:
                data['_id'] = docid
                if 'rev' in data:
                    data['_rev'] = data.pop('rev')
                cls._allow_dynamic_properties = dynamic_properties
                return cls.wrap(data)

        db = self._entries[dbname]
        return db.view(view, wrapper=default_wrapper, **params)

    def save(self, dbname, doc):
        """Saves a document to the given database"""
        if not isinstance(doc, Document):
            raise TypeError("doc must be a document")

        db = self._entries[dbname]
        doc_json = doc.to_json()

        db.save_doc(doc_json)
        if '_id' in doc and '_rev' in doc:
            doc._doc.update(doc)
        elif '_id' in doc:
            doc._doc.update(dict(id=doc['_id']))


