const mongoose = require("mongoose");
const StreamArray = require("stream-json/streamers/StreamArray");
const delay = require("delay");
const { config } = require("../../config");
const fs = require("fs");
const Data = require("../commons/global");
var EJSON = require("mongodb-extended-json");

const jsonStream = StreamArray.withParser();
const collectionSchema = mongoose.Schema({}, { strict: false });
const Model = mongoose.model(config.collectionName, collectionSchema);

fs.createReadStream(config.dataForMigrate).pipe(jsonStream.input);

const startMigration = () => {
  try {
    const db = mongoose.connection;

    db.on("error", console.error.bind(console, "connection error: "));

    db.once("open", function () {
      console.log("Connection Successful!");
      const dataToMigrate = [];
      let countItem = 0;
      jsonStream.on("data", async ({ key, value }) => {
        const data = EJSON.parse(JSON.stringify(value));
        jsonStream.pause();
        const existData = await Model.exists({
          _id: mongoose.Types.ObjectId(data._id),
        });

        if (!existData) {
          dataToMigrate.push(data);
          console.log(
            `Quantity: ${countItem + 1} - Save ${data._id} data for migrate 💾`
          );
        } else {
          console.log(`✅ Quantity: ${countItem + 1} - Data exist`);
        }
        countItem++;
        jsonStream.resume();
      });

      jsonStream.on("end", async () => {
        console.log("Initialization the migration 🚀");
        await migrate(dataToMigrate);
        console.log("All Done 🤯");
      });
    });
  } catch (e) {
    console.error(e.message);
  }
};

async function migrate(dataToMigrate) {
  for (var data of dataToMigrate) {
    await save(data);
  }
}

async function saveSegmentData(data) {
  const document = new Model({ _id: mongoose.Schema.Types.ObjectId(data._id) });
  await document.save();
  const documentProperties = Object.keys(data);
  const documentPropertiesValues = Object.values(data);
  for (let key in documentProperties) {
    if (documentProperties[key] !== "_id") {
      if (documentProperties[key] === "items")
        documentPropertiesValues[key] = [];
      await Model.findOneAndUpdate(
        { _id: mongoose.Types.ObjectId(data._id) },
        { $set: Object.assign({}, documentPropertiesValues[key]) },
        { new: true },
        async function (err) {
          if (err) {
            console.log(err);
            Data.global.errorCode = err.code;
            console.log(`Error 🔴: ObjectId: ${data._id} Error: ${err}`);
          } else {
            Data.global.errorCode = 0;
            console.log(`🟢 Created Successfuly`);
          }
        }
      );
    }
  }
}
async function save(data) {
  const result = new Model(data);
  await delay(config.timeForRetry);
  await result.save(async function (err) {
    if (err) {
      console.log(err);
      Data.global.errorCode = err.code;
      console.log(`Error 🔴: ObjectId: ${data._id} Error: ${err}`);
      // if (err.code === 16) {
      //   console.log("🟠 🔄 Retry with segment data");
      //   await saveSegmentData(data);
      // }
    } else {
      Data.global.errorCode = 0;
      console.log(`🟢 Created Successfuly`);
    }
  });
}

module.exports = startMigration;
