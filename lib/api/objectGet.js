'use strict';

const utils = require('../utils.js');
const services = require('./services.js');
const async = require('async');


/**
 * GET Object - Get an object
 * @param  {string} accessKey - user's access key
 *
 *
 *
 */

let objectGet = function(accessKey, datastore,  metastore, request, callback){
  let bucketname = utils.getResourceNames(request).bucket;
  let bucketUID = utils.getResourceUID(request.namespace, bucketname);
  let objectKey = utils.getResourceNames(request).object;
  let objectUID = utils.getResourceUID(request.namespace, bucketname + objectKey);
  //COMPLETE WITH NEEDED PARAMS
  let metadataValParams = {accessKey: accessKey, bucketUID: bucketUID, objectUID: objectUID, metastore: metastore};
  let datastoreParams = {};


  //Go to metastore and pull whether user is authorized and location of object in data store
  //Pull object from data store
  //Construct response

async.waterfall([
      function(next){
          services.metadataValidateAuthorization(metadataValParams, next)
      },
      function(bucket, objectExistsDatastoreLocation, next){
      	//CONTINUE HERE
          services.metadataGetObject(metadataParams, next)
      },
      function(bucket, objectExistsDatastoreLocation, next){
          services.dataStoreGetObject(metadataInfo, datastore, datastoreParams, next)
      }
  ], function (err, result) {
			return callback(err, result);
  });
};

module.exports = objectGet;