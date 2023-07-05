const config = require("dotenv").config();
const AWS = require("aws-sdk");
const Readable = require("stream").Readable;
const csv = require("csv-parser");
const log = require("loglevel");
log.setLevel(process.env.LOGLEVEL);
const zlib = require("zlib");
const db = require("./db");

const s3 = new AWS.S3({
  apiVersion: "2006-03-01",
  region: "us-east-2",
});

const s3BucketName = `${process.env.AWS_S3_BUCKET_NAME}`;

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

  let inserts = [];
  await new Promise((resolve, reject) => {
    return zlib.unzip(data.Body, (err, decompressed) => {
      if (err) {
        log.debug(`Error decompressing file: ${err}`);
      }

      // read the decompressed buffer to a readable stream
      const buffer = Readable.from(decompressed);
      return buffer
        .pipe(
          csv({
            skipLines: 2,
            separator: "\t",
            headers: headers, // Defined below
          })
        )
        .on("data", (row) => {
          // log.debug(row);
          // log.debug(row["cs-uri-stem"]);
          const uriStem = row["cs-uri-stem"];
          const isTrack = row["cs-uri-stem"].match(/track/);
          const trackKey = uriStem.match(
            /[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}/
          );
          const contentLength = parseInt(row["sc-content-len"]);
          const contentRangeEnd = parseInt(row["sc-range-end"]);

          // Conditional to filter out incidental requests
          if (
            isTrack &&
            contentLength > 2 &&
            contentLength >= contentRangeEnd
          ) {
            inserts.push({
              track_id: `${trackKey}`,
              user_id: row["cs(Referer)"],
              created_at: `${row["date"]} ${row["time"]}`,
              complete: true,
            });
          }
        })
        .on("end", () => {
          if (inserts.length > 0) {
            return db
              .knex("play")
              .insert(inserts)
              .then((result) => {
                // log.debug(result);
                log.debug("Rows processed");
                resolve();
              })
              .catch((err) => {
                log.debug(err);
                reject(err);
              });
          } else {
            log.debug("No rows to process");
            resolve();
          }
        });
    });
  });
};

const headers = [
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
];
