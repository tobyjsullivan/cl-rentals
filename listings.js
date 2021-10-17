import { open } from "fs/promises";
import { compose, Transform } from "stream";
import parse from "csv-parse";
import stringify from "csv-stringify";

const LISTINGS_CSV = "./data/listings.csv";

export const listing = (
  postId,
  url,
  price,
  bedrooms,
  sqft,
  latitude,
  longitude,
  accuracy,
  streetAddress,
  title,
  description,
  posted,
  updated,
  removed,
  flagged,
  fetched
) => ({
  postId,
  url,
  price,
  bedrooms,
  sqft,
  latitude,
  longitude,
  accuracy,
  streetAddress,
  title,
  description,
  posted,
  updated,
  removed,
  flagged,
  fetched,
});

const toCsvRow = ({
  postId,
  url,
  price,
  bedrooms,
  sqft,
  latitude,
  longitude,
  accuracy,
  streetAddress,
  title,
  description,
  posted,
  updated,
  removed,
  flagged,
  fetched,
}) => [
  postId,
  url,
  price,
  bedrooms,
  sqft,
  latitude,
  longitude,
  accuracy,
  streetAddress,
  title,
  description,
  posted,
  updated,
  removed,
  flagged,
  fetched,
];

const fromCsvRow = ([
  postId,
  url,
  price,
  bedrooms,
  sqft,
  latitude,
  longitude,
  accuracy,
  streetAddress,
  title,
  description,
  posted,
  updated,
  removed,
  flagged,
  fetched,
]) => ({
  postId,
  url,
  price,
  bedrooms,
  sqft,
  latitude,
  longitude,
  accuracy,
  streetAddress,
  title,
  description,
  posted,
  updated,
  removed,
  flagged,
  fetched,
});

export const readListingsStream = async () => {
  console.log("Opening listings CSV file: ", LISTINGS_CSV);
  const file = await open(LISTINGS_CSV, "a+");
  const readStream = file.createReadStream();
  readStream.on("end", () => {
    console.log("Closing input file...");
    file.close();
  });

  const csvParser = parse();

  const fromCsvTransform = new Transform({
    objectMode: true,
    transform(csvRow, encoding, callback) {
      const listing = fromCsvRow(csvRow);
      callback(undefined, listing);
    },
  });

  return readStream //
    .pipe(csvParser) //
    .pipe(fromCsvTransform);
};

export const writeListingsStream = async () => {
  const toCsvTransform = new Transform({
    objectMode: true,
    transform(listing, encoding, callback) {
      const csvRow = toCsvRow(listing);
      callback(undefined, csvRow);
    },
  });

  const csvStringifier = stringify();

  console.log("Opening listings CSV for append: ", LISTINGS_CSV);
  const file = await open(LISTINGS_CSV, "a");
  const writeStream = file.createWriteStream();
  writeStream.on("finish", () => {
    console.log("Closing output file...");
    file.close();
  });

  return compose(toCsvTransform, csvStringifier, writeStream);
};
