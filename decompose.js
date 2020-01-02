const mongo = require('mongodb');
const { MongoClient, ObjectID } = mongo;
const assert = require('assert');
const _ = require('lodash');

const url = 'mongodb://localhost:27017';
const dbName = 'marvel';
const client = new MongoClient(url);
const batchSize = 10;
let count = 0;
let total = 0;



async function asyncForEach(array, callback) {
  for (let index = 0; index < array.length; index++) {
    await callback(array[index], index, array);
  }
}

const getCollection = (db) => (name) => {
  return db.collection(name);
}

const createRef = async (coll, value) => {
  const match = await coll.findOne(value);
  let ref = match ? match._id : null;
  if (!match) {
    const updateResult = await coll.findOneAndUpdate(value, { $set: value }, { upsert: true });
    if (updateResult.value) {
      ref = updateResult.value._id;
    } else if (updateResult.upserted && ObjectID.isValid(updateResult.upserted)) {
      ref = updateResult.upserted;
    } else if (updateResult.lastErrorObject && ObjectID.isValid(updateResult.lastErrorObject.upserted)) {
      ref = updateResult.lastErrorObject.upserted;
    } else {
      console.log('updateResult', updateResult);
    }
  }
  if (!ref) console.log('no ref!')
  return ref;
}

const populator = (srcColl, targetColl) => async (doc, field) => {
  const fieldValue = doc[field];
  if (_.isPlainObject(fieldValue)) {
    const tgtColl = targetColl(field);
    const refId = await createRef(tgtColl, fieldValue);
    if (!refId) return console.log('no ref!');
    await srcColl.findOneAndUpdate({ _id: doc._id }, { $set: { [field]: refId } });
  }
  if (_.isArray(fieldValue)) {
    const tgtColl = targetColl(field);
    let newValues = [];
    await asyncForEach(fieldValue, async (value) => {
      const pos = _.indexOf(fieldValue, value);
      const refId = await createRef(tgtColl, value);
      if (pos < 0 || !refId) return console.log('pos < 0 || !refId', pos < 0, !refId);
      newValues.push(refId);
    });
    await srcColl.findOneAndUpdate({ _id: doc._id }, { $set: { [field]: newValues } })
  }
};

const decomposeDoc = (db) => async (doc, srcColl) => {
  const targetColl = getCollection(db);
  const populate = populator(srcColl, targetColl);
  await asyncForEach(Object.keys(doc), async field => {
    const fieldValue = doc[field];
    if (!ObjectID.isValid(fieldValue) && _.isObjectLike(fieldValue)) await populate(doc, field);
  });
  count += 1;
  if (count % batchSize < 1) console.log(`${count} out of ${total}`, doc._id);
  return;
}

const decomposeCollection = (db) => async (srcColl) => {
  const decompose = decomposeDoc(db);
  total = await srcColl.countDocuments();
  let skip = 0;
  while (skip < total) {
    const cursor = await srcColl.find({}).skip(skip).limit(batchSize);
    await cursor.forEach(async doc => await decompose(_.clone(doc), srcColl));
    skip += batchSize;
  }
}

client.connect(async function (err) {
  assert.equal(null, err);
  const db = client.db(dbName);
  const collection = getCollection(db);
  const decompose = decomposeCollection(db);

  await decompose(collection('comics'));
  await decompose(collection('characters'));
  await client.close();
  process.exit(0);
});
