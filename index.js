/**
 * -Module expects AWS credentials (S3 specifically) to be available as specified by AWS Node.js sdk setup
 * -This module takes the the image from the specified s3 bucket and key, saves them in the local
 * directory (if not in lambda), processes them, and re-uploads back to s3
 * - Run .config to configure the upload_folder and other settings
 */

const BPromise       = require('bluebird');
const cuid           = require('cuid');
const fs             = require('fs');
const path           = require('path');
const mkdirp         = require('mkdirp');
const rimraf         = require('rimraf');
const awsWrapper     = new (require('aws_wrapper').AwsWrapper)();
const imageFunctions = require('image_functions');
const ajv            = require('ajv')({
  removeAdditional: false
});

const module_config = {
  upload_folder: '/tmp/images/'
};

BPromise.promisifyAll(fs);

/**
 *
 * @param {object} opts
 * @param {string} opts.upload_folder
 */
exports.config = function (opts) {
  const schema = {
    type                : 'object',
    additionalProperties: false,
    properties          : {
      upload_folder: {
        type     : 'string',
        minLength: 1
      }
    }
  };
  
  return BPromise.resolve()
    .then(function () {
      const valid = ajv.validate(schema, opts);
      
      if (!valid) {
        const e = new Error(ajv.errorsText());
        
        e.ajv = ajv.errors;
        throw e;
      }
    })
    .then(function () {
      
      if (opts.upload_folder) {
        module_config.upload_folder = opts.upload_folder;
      }
      
      return true;
    });
};

/**
 *
 * @param {object} event
 * @param {string} event.processImageFunction
 * @param {string} event.s3_bucket_name
 * @param {string} event.s3_key
 * @param {string} event.s3_output_dir
 * @param {object[]} event.versions
 * @param {object} context
 * @returns {*}
 */
exports.handler = function (event, context) {
  
  const schema = {
    type                : 'object',
    additionalProperties: false,
    required            : ['processImageFunction', 's3_key', 's3_bucket_name', 's3_output_dir'],
    properties          : {
      processImageFunction: {
        enum: ['processImage3', 'processImage4']
      },
      s3_key              : {
        type     : 'string',
        minLength: 1
      },
      s3_bucket_name      : {
        type     : 'string',
        minLength: 1
      },
      s3_output_dir       : {
        type     : 'string',
        minLength: 1
      },
      versions            : {
        type: 'array' // see imageFunctions for complete format
      }
    }
  };
  
  const opts = {
    file_name : null,
    input_dir : path.join(module_config.upload_folder, `/input-${cuid()}/`),
    output_dir: path.join(module_config.upload_folder, `/output-${cuid()}/`)
  };
  
  const return_value = {};
  
  return BPromise.resolve()
    .then(function () {
      
      const valid = ajv.validate(schema, event);
      
      if (!valid) {
        const e = new Error(ajv.errorsText());
        
        e.ajv = ajv.errors;
        throw e;
      }
      
      console.log('lambda_process_images: Validation complete');
      
      return true;
      
    })
    .then(function () {
      // ensure the input and output directories exist
      
      return BPromise.resolve()
        .then(function () {
          
          return new BPromise(function (resolve, reject) {
            
            mkdirp(opts.input_dir, function (err) {
              if (err) {
                reject(err);
              }
              else {
                resolve(true);
              }
            });
            
          });
          
        })
        .then(function () {
          
          return new BPromise(function (resolve, reject) {
            
            mkdirp(opts.output_dir, function (err) {
              if (err) {
                reject(err);
              }
              else {
                resolve(true);
              }
            });
            
          });
          
        });
      
    })
    .then(function () {
      opts.file_name = path.basename(event.s3_key);
      console.log(`lambda_process_images: file_name = ${opts.file_name}`);
      
      return true;
    })
    .then(function () {
      // get the object
      console.log('lambda_process_images: Getting object from s3');
      
      return awsWrapper.S3Wrapper.getObject({
        s3_bucket_name: event.s3_bucket_name,
        s3_key        : event.s3_key
      })
        .then(function (buffer) {
          
          console.log('lambda_process_images: Got object from s3');
          
          return fs.writeFileAsync(path.join(opts.input_dir, opts.file_name), buffer);
        });
    })
    .then(function () {
      //process
      console.log('lambda_process_images: Starting image processing');
      
      const process_opts = {
        dir       : opts.input_dir,
        output_dir: opts.output_dir,
        versions  : event.versions
      };
      
      return imageFunctions[event.processImageFunction](process_opts);
    })
    .then(function (resp) {
      
      /*
       * FORMAT, for an image 'test_image1.jpg', the function returns an object
       * { 'test_image1.jpg': [
       'test_image1_aspR_2.038_w815_h400_e.jpg',
       'test_image1_aspR_2.038_w815_h400_e400.jpg',
       'test_image1_aspR_2.038_w815_h400_e80.jpg',
       'test_image1_aspR_2.038_w815_h400_e200.jpg'
       ] }
       *
       * */
      
      // we only keep track of the original key since we can use it to derive all other versions
      return_value.data = {
        key: path.join(event.s3_output_dir, resp[opts.file_name][0])
      };
      
      console.log('lambda_process_images: Finished image processing');
      
      return true;
    })
    .then(function () {
      
      console.log('lambda_process_images: Uploading back to S3');
      
      return awsWrapper.S3Wrapper.uploadFiles({
        dir           : opts.output_dir,
        s3_bucket_name: event.s3_bucket_name,
        s3_output_dir : event.s3_output_dir,
        acl           : 'public-read',
        CacheControl  : 15552000
      })
        .then(function () {
          console.log('lambda_process_images: Successfully uploaded back to S3');
          
          return true;
        });
    })
    .then(function () {
      // delete the directories
      
      console.log('lambda_process_images: Cleaning up');
      
      return BPromise.resolve()
        .then(function () {
          
          return new BPromise(function (resolve, reject) {
            rimraf(opts.input_dir, function (e) {
              if (e) {
                reject(e);
              }
              else {
                resolve(true);
              }
            });
          });
          
        })
        .then(function () {
          
          return new BPromise(function (resolve, reject) {
            rimraf(opts.output_dir, function (e) {
              if (e) {
                reject(e);
              }
              else {
                resolve(true);
              }
            });
          });
          
        })
        .then(function () {
          console.log('lambda_process_images: Finished cleaning up');
        });
    })
    .then(function () {
      console.log('lambda_process_images: Returning');
      
      return context.succeed(return_value);
    });
  
};