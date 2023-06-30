const config = require("dotenv").config();
const AWS = require("aws-sdk");
const fs = require("fs");
const Readable = require("stream").Readable;
const csv = require("csv-parser");
const log = require("loglevel");
log.setLevel(process.env.LOGLEVEL);
const zlib = require("zlib");

const s3 = new AWS.S3({
  apiVersion: "2006-03-01",
  region: "us-east-2",
});

const s3BucketName = `${process.env.AWS_S3_BUCKET_NAME}`;
const trackPrefix = `${process.env.AWS_S3_TRACK_PREFIX}`;
const localConvertPath = `${process.env.LOCAL_CONVERT_PATH}`;
const localUploadPath = `${process.env.LOCAL_UPLOAD_PATH}`;

// Handler
exports.handler = async function (event, context) {
  const objectKey = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, " ")
  );

  log.debug(`objectKey: ${objectKey}`);

  if (!objectKey) {
    log.debug("No objectKey");
    return;
  }

  log.debug("Downloading file from S3");
  const getObject = () => {
    return new Promise((resolve, reject) => {
      const params = {
        Bucket: s3BucketName,
        Key: objectKey,
      };

      log.debug(`Downloading ${objectKey} from S3`);
      // Download file
      const content = s3.getObject(params, function (err, data) {
        if (err) {
          reject(err);
        } else {
          resolve(data);
        }
      });
    });
  };

  const data = await getObject();

  log.debug("Decompressing file");
  zlib.unzip(data.Body, (err, decompressed) => {
    if (err) {
      log.debug(`Error decompressing file: ${err}`);
    }
    console.log(decompressed);
    // read the decompressed buffer to a readable stream
    const buffer = Readable.from(decompressed);
    buffer
      .pipe(
        csv({
          skipLines: 2,
          separator: "\t",
          headers: [
            "date",
            "time",
            "x-edge-location",
            "sc-bytes",
            "c-ip",
            "cs-method",
            "cs(Host)",
            "cs-uri-stem",
            "sc-status",
            "cs(Referer)",
            "cs(User-Agent)",
            "cs-uri-query",
            "cs(Cookie)",
            "x-edge-result-type",
            "x-edge-request-id",
            "x-host-header",
            "cs-protocol",
            "cs-bytes",
            "time-taken",
            "x-forwarded-for",
            "ssl-protocol",
            "ssl-cipher",
            "x-edge-response-result-type",
            "cs-protocol-version",
            "fle-status",
            "fle-encrypted-fields",
            "c-port",
            "time-to-first-byte",
            "x-edge-detailed-result-type",
            "sc-content-type",
            "sc-content-len",
            "sc-range-start",
            "sc-range-end",
          ],
        })
      )
      .on("data", (row) => {
        // log.debug(row);
        log.debug(row["cs-uri-stem"]);
        // TODO: Write row to database
      })
      .on("end", () => {
        log.debug("CSV file successfully processed");
      });
  });
};
