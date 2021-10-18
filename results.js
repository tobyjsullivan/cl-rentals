import { open } from "fs/promises";
import { compose, Transform } from "stream";
import parse from "csv-parse";
import stringify from "csv-stringify";

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

export class Results {
  constructor(csvFile) {
    this.csvFile = csvFile;
  }

  async writeResultsStream() {
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

    console.log("Opening for append: ", this.csvFile);
    const file = await open(this.csvFile, "a");
    const writeStream = file.createWriteStream();
    writeStream.on("finish", () => {
      console.log("Closing output file...");
      file.close();
    });

    return compose(
      toCsvTransform,
      csvStringifier,
      loggingTransform,
      writeStream
    );
  }

  async readResultsStream() {
    console.log("Opening input file: ", this.csvFile);
    const file = await open(this.csvFile, "r");
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
  }
}
