var sys = require('sys');
var path = require('path');

var resourcer = require('resourcer'),
    cradle = require('cradle');

resourcer.Cache = require('resourcer/cache').Cache;

this.Connection = function (config) {
    this.connection = new(cradle.Connection)({
        host:  config.host || '127.0.0.1',
        port:  config.port || 5984,
        raw:   true,
        cache: false,
        auth:  config && config.auth || null
    }).database(config.uri || resourcer.env); 
    this.cache = new(resourcer.Cache);
};

this.Connection.prototype = {
    protocol: 'database',
    load: function(data) {
				throw new(Error)("Load not valid for database engine.");
    },
    request: function (method) {
        var args = Array.prototype.slice.call(arguments, 1);
        return this.connection[method].apply(this.connection, args);
    },
    head: function (id, callback) {
        return this.request('head', id, callback);
    },
    get: function (id, callback) {
        this.request.call(this, 'get', id, function (e, res) {
            if (e) { callback(e) }
            else {
                if (Array.isArray(id)) {
                    callback(null, res.rows.map(function (r) { return r.doc }));
                } else {
                    callback(null, res);
                }
            }
        });
    },
    put: function (id, doc, callback) {
        var args = Array.prototype.slice.call(arguments);
        return this.request('put', id, doc, function (e, res) {
            if (e) {
                callback(e);
            } else {
                res.status = 201;
                callback(null, res);
            }
        });
    },
    save: function () {
        return this.put.apply(this, arguments);
    },
    update: function (id, doc, callback) {
        var that = this;
        
        if (this.cache.has(id)) {
            if (!doc._rev) delete doc._rev;
            that.put(id, resourcer.mixin({}, this.cache.get(id), doc), callback);
        } else {
            this.get(id, function (e, res) {
                var obj = resourcer.mixin({}, res, doc);
                that.put(id, obj, callback);
            });
        }
    },
    destroy: function () {
        var that = this,
            args = Array.prototype.slice.call(arguments),
            id = args.shift();
        
        if (this.cache.has(id)) {
            args = [id, this.cache.get(id)._rev].concat(args);
            return this.request.apply(this, ['remove'].concat(args));
        } else {
            this.get(id, function (e, res) {
                args = [id, res._rev].concat(args);
                return that.request.apply(that, ['remove'].concat(args));
            });
        }
    },
    view: function (path, opts, callback) {
        return this.request.call(this, 'view', path, opts, function (e, res) {
            if (e) { callback(e) }
            else {
                callback(null, res.rows.map(function (r) {
                    // With `include_docs=true`, the 'doc' attribute is set instead of 'value'.
                    var doc = r.doc || r.value;

                    if (r.id) { doc._id = r.id }
                    return doc;
                }));
            }
        });
    },
    all: function (callback) {
        return this.request.call(this, 'all', { include_docs: 'true' }, function (e, res) {
            if (e) { callback(e) }
            else {
                callback(null, res.rows.map(function (r) { return r.doc }));
            }
        });
    }
};
