# Ouch, prototype driver for CouchDB/PouchDB

To run "gopherjs test -v" you must "npm install pouchdb" (which actually runs on top of levelDB). 
Also need to "npm install pouchdb-find" for queries.
When running "gopherjs test" from the ouch directory, a pouchdb.test directory will be created (in .gitignore).


To run "go test" a publiclly accessable CouchDB must be at "http://127.0.0.1:5984/ouchtest" with an administrator account testusername:testpassword (basic auth only, as this also works from the browser).

Note from Discussion forum, re future nosql API alterations at https://discourse.cayley.io/t/running-cayley-in-the-browser/960/13

* I suggest that all the Database interface methods have context as the first parameter, and that context is removed from all the "child" methods (like Query.One).
* Reply from dennwc37: This might not work well for DocIterator - it might be passed around and should use the context of the last caller, not the first one. I would say it make sense we can add contexts to Insert, FindByKey and EnsureIndexes and leave other builders as-is, since they already pass context in Do, One, Next, etc.

## TODO

* fix bugs reported by TestAll
* fix "KK" bug in Delete.Keys()
* work out how to delete test db remotely from JS using PouchDB as front-end to CouchDB
* add indexing
* optimize ID field to be simple string
* only pull back the _id & _ref fields in the delete query
	