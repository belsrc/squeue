'use strict';

const Promise = require('bluebird');
const MongoClient = require('mongodb').MongoClient;
const ObjectID = require('mongodb').ObjectID;

const defaultOptions = {
  collection: 'queue',
  release: 30,
  retries: 5,
  mongo: {
    keepAlive: 20000,
    autoReconnect: true,
  },
};

const defaultDoc = {
  locked: false,
  locked_at: null,
  retries: 0,
  complete: false,
  completed_at: null,
  dead: false,
};

/**
 * Initializes the queue collection.
 * @private
 * @param  {Object}  collection  The Mongo collection object.
 * @return {Promise}
 */
function initCollection(collection) {
  return new Promise((resolve, reject) => {
    collection.createIndexes([
      { key: { locked: -1, complete: -1, dead: -1, priority: -1, created_at: 1 }, name: 'read_index' },
      { key: { completed_at: 1 }, name: 'ttl_index', expireAfterSeconds: 604800 },
    ], error => {
      if(error) {
        return reject(error);
      }

      return resolve();
    });
  });
}

/**
 * Checks whether the queue collection exists.
 * @private
 * @param  {Object}  db    The Mongo DB object.
 * @param  {String}  name  The name of the queue collection.
 * @return {Promise} Promise of whether or not the collection exists.
 */
function hasCollection(db, name) {
  return new Promise((resolve, reject) => {
    db
      .listCollections({ name })
      .next((error, collinfo) => {
        if(error) {
          return reject(error);
        }

        if(collinfo) {
          return resolve(true);
        }

        return resolve(false);
      });
  });
}

/**
 * Creates the queue collection.
 * @private
 * @param  {Object}  db    The Mongo DB object.
 * @param  {String}  name  The name of the queue collection.
 * @return {Promise} Promise of the Mongo collection object.
 */
function createCollection(db, name) {
  return new Promise((resolve, reject) => {
    db.createCollection(name, (error, collection) => {
      if(error) {
        return reject(error);
      }

      return resolve(collection);
    });
  });
}

/**
 * Gets the Mongo collection object.
 * @private
 * @param  {Object}  db    The Mongo DB object.
 * @param  {String}  name  The name of the queue collection.
 * @return {Promise} Promise of the Mongo collection object.
 */
function getCollection(db, name) {
  return hasCollection(db, name)
    .then(hasCol => {
      if(hasCol) {
        return new Promise((resolve, reject) => {
          db.collection(name, { strict: true }, (error, coll) => {
            if(error) {
              return reject(error);
            }

            return resolve(coll);
          });
        });
      }

      return createCollection(db, name);
    });
}

/**
 * Create a Mongo connection.
 * @private
 * @param  {String}  conString Mongo connection string.
 * @param  {Object}  options   Mongo connection options.
 * @return {Promise} Promise of the Mongo DB connection.
 */
function getConnection(conString, options) {
  return new Promise((resolve, reject) => {
    MongoClient.connect(conString, options, (error, db) => {
      if(error) {
        return reject(error);
      }

      return resolve(db);
    });
  });
}

/**
 * The Queue class.
 * @class
 */
class Queue {
  /**
   * Initializes a new instance of the Queue class.
   * @param {String}  conString           The Mongo connection string.
   * @param {Object}  options             The options object.
   * @param {String}  options.collection  The collection name for the queue.
   * @param {Number}  options.release     The number of seconds a queue can run before being considered old.
   * @param {Number}  options.retries     The number of retries before a queue is dead.
   * @param {Object}  options.mongo       The Mongo connection options.
   */
  constructor(conString, options) {
    options = options || {};

    if(!conString || typeof conString !== 'string') {
      throw new Error('Mongo connection string is required');
    }

    this.collectionName = options.collection || defaultOptions.collection;
    this.releaseTime = options.release || defaultOptions.release;
    this.maxRetries = options.retries || defaultOptions.retries;
    this.mongoOptions = options.mongo || defaultOptions.mongo;
    this.connString = conString;
  }

  /**
   * Opens a connection to the Mongo DB.
   * @return {Promise}  Promise of the DB object.
   */
  connect() {
    const self = this;

    if(this.collection) {
      return Promise.resolve();
    }

    return getConnection(this.connString, this.mongoOptions)
      .then(con => {
        self.db = con;

        return getCollection(self.db, self.collectionName)
          .then(collection => {
            self.collection = collection;
            return initCollection(self.collection);
          });
      });
  }

  /**
   * Adds a message to the queue.
   * @param  {Mixed}    message    The message to queue.
   * @param  {Number}   [priority] The message priority.
   * @return {Promise}  Promise of the added document.
   */
  add(message, priority) {
    if(!message) {
      return Promise.reject(new Error('must supply a queue message'));
    }

    priority = priority || 1;

    return this
      .connect()
      .then(() => {
        const doc = JSON.parse(JSON.stringify(defaultDoc));

        doc.message = message;
        doc.priority = priority;
        doc.created_at = new Date();

        return new Promise((resolve, reject) => {
          this.collection.insertOne(doc, (error, response) => {
            if(error) {
              return reject(error);
            }

            return resolve(response.ops[0]);
          });
        });
      });
  }

  /**
   * Gets the next item in the queue.
   * @return {Promise}  Promise of the queue message.
   */
  get() {
    return this
      .connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.collection.findOneAndUpdate(
            { locked: false, complete: false, dead: false },
            { $set: { locked: true, locked_at: new Date() } },
            { new: true, sort: { priority: -1, created_at: 1 } },
            (error, doc) => {
              if(error) {
                return reject(error);
              }

              const data = {
                id: doc.value._id,
                message: doc.value.message,
              };

              return resolve(data);
            }
          );
        });
      });
  }

  /**
   * Marks a queue item as complete.
   * @param  {String}   id  The item ID.
   * @return {Promise}  An empty Promise.
   */
  complete(id) {
    return this
      .connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.collection.findOneAndUpdate(
            { _id: new ObjectID(id) },
            { $set: { locked: false, complete: true, completed_at: new Date() } },
            { new: true },
            error => {
              if(error) {
                return reject(error);
              }

              return resolve();
            }
          );
        });
      });
  }

  /**
   * Marks a queue item as failed.
   * @param  {String}   id  The item ID.
   * @return {Promise}  An empty Promise.
   */
  fail(id) {
    const self = this;

    return this
      .connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.collection.findOneAndUpdate(
            { _id: new ObjectID(id) },
            { $set: { locked: false }, $inc: { retries: 1 } },
            { new: true },
            error => {
              if(error) {
                return reject(error);
              }

              return resolve();
            }
          );
        });
      })
      .then(doc => {
        if(doc.retries >= self.maxRetries) {
          return self.markDead(id);
        }
      });
  }

  /**
   * Marks a queue item as dead (to many retries).
   * @param  {String}   id  The item ID.
   * @return {Promise}  An empty Promise.
   */
  markDead(id) {
    return this
      .connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.collection.findOneAndUpdate(
            { _id: new ObjectID(id) },
            { $set: { dead: true } },
            { new: true },
            error => {
              if(error) {
                return reject(error);
              }

              return resolve();
            }
          );
        });
      });
  }

  /**
   * Removes all completed queue items.
   * The collection already has a TTL index on completed documents so this is merely if you
   * want to manually flush them.
   * @return {Promise}  Promise of the number of documents removed.
   */
  clean() {
    return this
      .connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.collection
            .deleteMany({ complete: true }, (error, response) => {
              if(error) {
                return reject(error);
              }

              return resolve(response.result.n);
            });
        });
      });
  }

  /**
   * Frees the queue items that are passed the release time.
   * @return {Promise}  Promise of the number of documents modified.
   */
  free() {
    const ms = this.releaseTime * 1000;
    const time = new Date(new Date().valueOf() - ms);

    return this
      .connect()
      .then(() => {
        return new Promise((resolve, reject) => {
          this.collection.updateMany(
            { locked: true, locked_at: { $lte: time } },
            { $set: { locked: false, locked_at: null } },
            (error, response) => {
              if(error) {
                return reject(error);
              }

              return resolve(response.result.nModified);
            }
          );
        });
      });
  }
}

/**
 * Gets a new instance of the {@link Queue} class.
 * @module
 */
module.exports = (conInfo, options) => {
  return new Queue(conInfo, options);
};
