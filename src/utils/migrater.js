const mongoose = require("mongoose");
const StreamArray = require("stream-json/streamers/StreamArray");
const delay = require('delay');
const { config } = require("../../config");
const fs = require("fs");
const Data = require('../commons/global');
var EJSON = require("mongodb-extended-json");

const jsonStream = StreamArray.withParser();

fs.createReadStream(config.dataForMigrate).pipe(jsonStream.input);

const startMigration = () => {
  try {
    const db = mongoose.connection;

    db.on("error", console.error.bind(console, "connection error: "));

    db.once("open", function () {
      console.log("Connection Successful!");
      const collectionSchema = mongoose.Schema({}, { strict: false });
      const Model = mongoose.model(config.collectionName, collectionSchema);
      jsonStream.on("data", async ({ key, value }) => {
        const data = EJSON.parse(JSON.stringify(value));
        const result = new Model(data);
        result.save(function (err) {
          if (err) {
            Data.global.errorCode = err.code;
            console.log(`Error 🔴: ObjectId: ${data._id} Error: ${err}`);
          }
          else {
            console.log(`🟢 Created Successfuly`);
          }
        });
        jsonStream.pause();
        if (Data.global.errorCode === 16500) {
            console.log('Waiting for retry 🕚');
            await delay(config.timeForRetry*10);
            setTimeout(() => {
                result.save(function (err) {
                    if (err) {
                      console.log(`Error 🔴: ObjectId: ${data._id} Error: ${err}`);
                      return;
                    }
                    console.log(`🟢 Created Successfuly`);
                  });
                jsonStream.resume();
            }, config.timeForRetry*5);
        }
        else {
            setTimeout(() => {
                jsonStream.resume();
            }, config.timeForRetry);
        }

      });

      jsonStream.on("end", () => {
        console.log("All Done 🤯");
      });
    });
  } catch (e) {
    console.error(e.message);
  }
};

function retrySave(data, callBackFunction) {
  console.log("🟠 🔄 Retry process");
  callBackFunction.save(function (err) {
    if (err) {
      console.log(`Error 🔴: ObjectId: ${data._id} Error: ${err}`);
      return;
    }
    console.log(`🟢 Created Successfuly`);
  });

}

module.exports = startMigration;
