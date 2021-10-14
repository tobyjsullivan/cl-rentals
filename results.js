import { open } from "fs/promises";
import { compose, Transform } from "stream";
import parse from "csv-parse";
import stringify from "csv-stringify";

const RESULTS_CSV = "./data/results.csv";

export const result = (
  runId,
  postId,
  title,
  posted,
  url,
  price,
  bedrooms,
  floorSqft
) => ({
  runId,
  postId,
  title,
  posted,
  url,
  price,
  bedrooms,
  floorSqft,
});

const toCsvArray = ({
  runId,
  postId,
  title,
  posted,
  url,
  price,
  bedrooms,
  floorSqft,
}) => [runId, postId, title, posted, url, price, bedrooms, floorSqft];

const fromCsvArray = ([
  runId,
  postId,
  title,
  posted,
  url,
  price,
  bedrooms,
  floorSqft,
]) => ({
  runId,
  postId,
  title,
  posted,
  url,
  price,
  bedrooms,
  floorSqft,
});

export const writeResultsStream = async () => {
  const toCsvTransform = new Transform({
    objectMode: true,
    transform(listing, encoding, callback) {
      const csvRow = toCsvArray(listing);
      callback(undefined, csvRow);
    },
  });

  const csvStringifier = stringify();

  let count = 0;
  const loggingTransform = new Transform({
    objectMode: true,
    transform(data, encoding, callback) {
      const parsed = data.toString(encoding);
      console.log("Seen %d: ", count++, parsed);
      // Pass along.
      callback(undefined, data);
    },
  });

  console.log("Opening for append: ", RESULTS_CSV);
  const file = await open(RESULTS_CSV, "a");
  const writeStream = file.createWriteStream();
  writeStream.on("finish", () => {
    console.log("Closing output file...");
    file.close();
  });

  return compose(toCsvTransform, csvStringifier, loggingTransform, writeStream);
};

export const readResultsStream = async () => {
  console.log("Opening input file: ", RESULTS_CSV);
  const file = await open(RESULTS_CSV, "r");
  const readStream = file.createReadStream();
  readStream.on("end", () => {
    console.log("Closing input file...");
    file.close();
  });

  let count = 0;
  const loggingTransform = new Transform({
    objectMode: true,
    transform(data, encoding, callback) {
      const parsed = data.toString(encoding);
      console.log("Found %d: ", count++, parsed);
      // Pass along.
      callback(undefined, data);
    },
  });

  const csvParser = parse();

  const fromCsvTransform = new Transform({
    objectMode: true,
    transform(csvRow, encoding, callback) {
      const result = fromCsvArray(csvRow);
      callback(undefined, result);
    },
  });

  return readStream
    .pipe(loggingTransform)
    .pipe(csvParser)
    .pipe(fromCsvTransform);
};
