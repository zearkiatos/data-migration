require('dotenv').config();

const config = {
    dev: process.env.NODE_ENV !== 'production',
    environment: process.env.NODE_ENV,
    port: process.env.PORT || 3000,
    connectionString: process.env.CONNECTION_MONGO_STRING,
    collectionName: process.env.COLLECTION_NAME,
    dataForMigrate: process.env.DATA_FOR_MIGRATE,
    timeForRetry: parseInt(process.env.TIME_FOR_RETRY)
};

module.exports = { config }