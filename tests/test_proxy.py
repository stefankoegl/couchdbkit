import unittest

from couchdbkit import *

from restkit import SimplePool                                                                                                                                                               
pool = SimplePool()

class DBProxyTestCase(unittest.TestCase):
    def setUp(self):
        self.server = Server(pool_instance=pool)
        self.db1 = self.server.create_db('couchdbkit_test')
        self.db2 = self.server.create_db('couchdbkit_test2')

    def tearDown(self):
        try:
            self.server.delete_db('couchdbkit_test')
            self.server.delete_db('couchdbkit_test2')
        except:
            pass

    def testAddDb(self):
        proxy = DBProxy(customer_1=self.db1)
        self.assertEquals(proxy.get_db('customer_1'), self.db1)
        self.assertRaises(KeyError, proxy.get_db, 'customer_2')
        self.assert_(not 'customer_2' in proxy)

        proxy.add_db('customer_2', self.db2)
        self.assert_('customer_2' in proxy)
        self.assertEquals(proxy.get_db('customer_2'), self.db2)

    def testAddDbFail(self):
        proxy = DBProxy()
        self.assertRaises(TypeError, proxy.add_db, 'customer_1', 'blah')
        self.assertRaises(TypeError, proxy.add_db, 'customer_1', 123)

    def testDeleteDb(self):
        proxy = DBProxy(customer_1=self.db1)
        self.assert_('customer_1' in proxy)
        proxy.delete_db('customer_1')

        proxy.add_db('customer_2', self.db2)
        self.assert_('customer_2' in proxy)
        self.assertEquals(proxy.get_db('customer_2'), self.db2)

    def testSave(self):
        class MyDoc(Document):
            s = StringProperty()

        proxy = DBProxy(customer_1=self.db1, customer_2=self.db2)
        mydoc = MyDoc(s="amazing test")
        proxy.save('customer_2', mydoc)
        self.assert_(mydoc._id)

        self.assertRaises(ResourceNotFound, self.db1.get, mydoc._id)
        mydoc_back = self.db2.get(mydoc._id)
        self.assert_(mydoc_back['_id'] == mydoc._id)
        self.assert_(mydoc_back['_rev'] == mydoc._rev)
        self.assert_(mydoc_back['s'] == mydoc.s)

    def testAttachment(self):
        class MyDoc(Document):
            s = StringProperty()

        proxy = DBProxy(customer_1=self.db1, customer_2=self.db2)
        mydoc = MyDoc(s="amazing test")
        proxy.save('customer_2', mydoc)
        self.assert_(mydoc._id)
        proxy.put_attachment('customer_2', mydoc, content='asdf\nfdsa',
                             name='test.txt')
        # Now fetch it back.
        self.assert_(proxy.fetch_attachment('customer_2', mydoc, 'test.txt') == 'asdf\nfdsa')

        # Delete it.
        proxy.delete_attachment('customer_2', mydoc, 'test.txt')
        self.assertRaises(ResourceNotFound, proxy.fetch_attachment, 'customer_2', mydoc, 'test.txt')

    def testGet(self):
        class MyDoc(Document):
            s = StringProperty()

        proxy = DBProxy(customer_1=self.db1, customer_2=self.db2)
        mydoc = MyDoc(s="amazing test")
        proxy.save('customer_2', mydoc)
        self.assert_(mydoc._id)

        mydoc_back = proxy.get('customer_2', MyDoc, mydoc._id)
        self.assert_(mydoc_back['_id'] == mydoc._id)
        self.assert_(mydoc_back['_rev'] == mydoc._rev)
        self.assert_(mydoc_back['s'] == mydoc.s)

    def testView(self):
        class TestDoc(Document):
            field1 = StringProperty()
            field2 = StringProperty()

        design_doc = {
            '_id': '_design/test',
            'language': 'javascript',
            'views': {
                'all': {
                    "map": """function(doc) { if (doc.doc_type == "TestDoc") { emit(doc._id, doc);
}}"""
                }
            }
        }
        doc = TestDoc(field1="a", field2="b")
        doc1 = TestDoc(field1="c", field2="d")

        proxy = DBProxy(customer_1=self.db1)
        self.db1.save_doc(design_doc)
        proxy.save('customer_1', doc)
        proxy.save('customer_1', doc1)

        results = proxy.view('customer_1', TestDoc, 'test/all')
        self.assert_(len(results) == 2)
        resall = results.all()
        self.assert_(resall[0].field1 == 'c')
        self.assert_(resall[0].field2 == 'd')
        self.assert_(resall[1].field1 == 'a')
        self.assert_(resall[1].field2 == 'b')

    def testViewWithLimit(self):
        class TestDoc(Document):
            field1 = StringProperty()
            field2 = StringProperty()

        design_doc = {
            '_id': '_design/test',
            'language': 'javascript',
            'views': {
                'all': {
                    "map": """function(doc) { if (doc.doc_type == "TestDoc") { emit(doc._id, doc);
}}"""
                }
            }
        }
        doc = TestDoc(field1="a", field2="b")
        doc1 = TestDoc(field1="c", field2="d")

        proxy = DBProxy(customer_1=self.db1)
        self.db1.save_doc(design_doc)
        proxy.save('customer_1', doc)
        proxy.save('customer_1', doc1)

        results = proxy.view('customer_1', TestDoc, 'test/all', limit=1)
        self.assert_(len(results) == 1)
        resall = results.all()
        self.assert_(resall[0].field1 == 'c')
        self.assert_(resall[0].field2 == 'd')

    def testDelete(self):
        class TestDoc(Document):
            field1 = StringProperty()
            field2 = StringProperty()

        doc = TestDoc(field1="a", field2="b")

        proxy = DBProxy(customer_1=self.db1)
        proxy.save('customer_1', doc)
        docfromdb = proxy.get('customer_1', TestDoc, doc._id)
        self.assert_(docfromdb._id == doc._id)

        # Now delete it.
        proxy.delete('customer_1', docfromdb)

        # Make sure it's gone.
        self.assertRaises(ResourceNotFound, proxy.get, 'customer_1', TestDoc, doc._id)

if __name__ == '__main__':
    unittest.main()

