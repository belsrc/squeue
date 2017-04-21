'use strict';

// NOTE: findAndModify has been deprecated in mongo-native, the new replacement methods, at this time, don't support
// the { new: true } option, so in lieu of that I simply don't return anything for the complete, fail and markDead methods.

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
  return collection.createIndexes([
    { key: { locked: -1, complete: -1, dead: -1, priority: -1, created_at: 1 }, name: 'read_index' },
    { key: { completed_at: 1 }, name: 'ttl_index', expireAfterSeconds: 604800 },
  ]);
}

/**
 * Checks whether the queue collection exists.
 * @private
 * @param  {Object}  db    The Mongo DB object.
 * @param  {String}  name  The name of the queue collection.
 * @return {Promise} Promise of whether or not the collection exists.
 */
function hasCollection(db, name) {
  return db
    .listCollections({ name })
    .toArray()
    .then(items => items.length >= 1);
}

/**
 * Creates the queue collection.
 * @private
 * @param  {Object}  db    The Mongo DB object.
 * @param  {String}  name  The name of the queue collection.
 * @return {Promise} Promise of the Mongo collection object.
 */
function createCollection(db, name) {
  return db.createCollection(name);
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
        return db.collection(name);
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
  return MongoClient.connect(conString, options);
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
    if(this.collection) {
      return Promise.resolve();
    }

    return getConnection(this.connString, this.mongoOptions)
      .then(con => {
        this.db = con;

        return getCollection(this.db, this.collectionName)
          .then(collection => {
            this.collection = collection;
            return initCollection(this.collection);
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
        const doc = Object.assign({}, defaultDoc, { created_at: new Date() });

        doc.message = message;
        doc.priority = priority;

        return this.collection
          .insertOne(doc)
          .then(result => result.ops[0]);
      });
  }

  /**
   * Gets the next item in the queue.
   * @return {Promise}  Promise of the queue message.
   */
  get() {
    return this
      .connect()
      .then(() => this.collection
          .findOneAndUpdate(
            { locked: false, complete: false, dead: false },
            { $set: { locked: true, locked_at: new Date() } },
            { new: true, sort: { priority: -1, created_at: 1 } }
          )
          .then(result => {
            const data = {
              id: result.value._id,
              message: result.value.message,
            };

            return data;
          })
      );
  }

  /**
   * Marks a queue item as complete.
   * @param  {String}   id  The item ID.
   * @return {Promise}  An empty Promise.
   */
  complete(id) {
    return this
      .connect()
      .then(() => this.collection
        .findOneAndUpdate(
          { _id: new ObjectID(id) },
          { $set: { locked: false, complete: true, completed_at: new Date() } },
          { new: true }
        )
      )
      .then(() => undefined);
  }

  /**
   * Marks a queue item as failed.
   * @param  {String}   id  The item ID.
   * @return {Promise}  An empty Promise.
   */
  fail(id) {
    return this
      .connect()
      .then(() => this.collection
        .findOneAndUpdate(
          { _id: new ObjectID(id) },
          { $set: { locked: false, locked_at: null }, $inc: { retries: 1 } },
          { new: true }
        )
      )
      .then(result => {
        const doc = result.value;

        if(doc.retries >= this.maxRetries) {
          return this.markDead(id);
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
      .then(() => this.collection
        .findOneAndUpdate(
          { _id: new ObjectID(id) },
          { $set: { dead: true } },
          { new: true }
        )
      )
      .then(() => undefined);
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
      .then(() => this.collection.deleteMany({ complete: true }))
      .then(response => response.result.n);
  }

  /**
   * Removes all dead queue items.
   * @return {Promise}  Promise of the number of documents removed.
   */
  bury() {
    return this
      .connect()
      .then(() => this.collection.deleteMany({ dead: true }))
      .then(response => response.result.n);
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
      .then(() => this.collection
        .updateMany(
          { locked: true, locked_at: { $lte: time } },
          { $set: { locked: false, locked_at: null } }
        )
      )
      .then(response => response.result.n);
  }
}

/**
 * Gets a new instance of the {@link Queue} class.
 * @module
 */
module.exports = (conInfo, options) => new Queue(conInfo, options);
