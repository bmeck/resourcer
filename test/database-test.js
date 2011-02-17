require.paths.unshift(require('path').join(__dirname, '..', 'lib'));

var path = require('path'),
    sys = require('sys'),
    assert = require('assert'),
    events = require('events'),
    http = require('http'),
    fs = require('fs'),
    vows = require('vows'),
    cradle = require('cradle'),
    resourcer = require('resourcer');

vows.describe('resourcer/engines/database').addVows({
    "A database containing default resources": {
        topic: function () {
            var promise = new(events.EventEmitter);
            var db = new(cradle.Connection)().database('test');
            db.destroy(function () {
                db.create(function () {
                    db.save([
                        { _id: 'bob', age: 35, hair: 'black'},
                        { _id: 'tim', age: 16, hair: 'brown'},
                        { _id: 'mat', age: 29, hair: 'black'}
                    ], function () {
                        promise.emit('success');
                    });
                });
            })
            return promise;
        },
        "is created": function () {}
    }
}).addVows({
    "A default Resource factory" : {
        topic: function() {
            return this.Factory = resourcer.defineResource(function () {
                this.use('database');
            });
        },
        "a create() request": {
            topic: function (r) {
                r.create({ _id: '99', age: 30, hair: 'red'}, this.callback);
            },
            "should return the newly created object": function (e, obj) {
                assert.instanceOf(obj, this.Factory);
                assert.equal(obj.id, '99');
            },
            "should create the record in the db": {
                topic: function (_, r) {
                    r.get(99, this.callback);
                },
                "which can then be retrieved": function (e, res) {
                    assert.isObject (res);
                    assert.equal    (res.age, 30);
                }
            }
        },
        "a get() request": {
            "when successful": {
                topic: function (r) {
                    return r.get('bob', this.callback);
                },
                "should respond with a Resource instance": function (e, obj) {
                    assert.isObject   (obj);
                    assert.instanceOf (obj, resourcer.resources.Resource);
                    assert.equal      (obj.constructor, resourcer.resources.Resource);
                },
                "should respond with the right object": function (e, obj) {
                    assert.isNull (e);
                    assert.equal  (obj._id, 'bob');
                }
            },
            "when unsuccessful": {
                topic: function (r) {
                    r.get("david", this.callback);
                },
                "should respond with an error": function (e, obj) {
                    assert.equal       (e.error, 'not_found');
                    assert.isUndefined (obj);
                }
            }
        },
        "an update() request": {
            "when successful": {
                topic: function (r) {
                    return r.update('bob', { age: 45 }, this.callback);
                },
                "should respond with 201": function (e, res) {
                    assert.isNull(e);
                    assert.equal(res.status, 201);
                }
            }
        }
    }
}).addBatch({
    "A default Resource factory" : {
        topic: function() {
            return this.Factory = resourcer.defineResource(function () {
                this.use('database');
            });
        },
        "an all() request": {
            topic: function (r) {
                r.all(this.callback);
            },
            "should respond with an array of all records": function (e, obj) {
                assert.isArray (obj);
                assert.length  (obj, 5);
            }
        }
    }
}).addBatch({
    "A default Resource factory" : {
        topic: function() {
            return this.Factory = resourcer.defineResource(function () {
                this.use('database');
            });
        },
        "an update() request": {
            topic: function (r) {
                this.cache = r.connection.cache;
                r.update('bob', { age: 36 }, this.callback);
            },
            "should respond successfully": function (e, res) {
                assert.isNull (e);
            },
            "followed by another update() request": {
                topic: function (_, r) {
                    r.update('bob', { age: 37 }, this.callback);
                },
                "should respond successfully": function (e, res) {
                    assert.isNull (e);
                },
                "should save the latest revision to the cache": function (e, res) {
                    assert.equal (this.cache.store['bob'].age, 37);
                    assert.match (this.cache.store['bob']._rev, /^4-/);
                }
            }
        }
    }
}).export(module);
