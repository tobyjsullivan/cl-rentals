const { readFile, writeFile, access, mkdir, open } = require("fs/promises");
const puppeteer = require("puppeteer");
const stringify = require("csv-stringify");
const { Readable, Transform, pipeline } = require("stream");
const path = require("path");

const START_URL =
  "https://vancouver.craigslist.org/d/apartments-housing-for-rent/search/apa";

const DATA_DIR = "./data";
const LAST_RUN_FILE = path.join(DATA_DIR, "/last_run");
const RUNS_CSV = path.join(DATA_DIR, "/runs.csv");
const RESULTS_CSV = path.join(DATA_DIR, "/results.csv");
const LISTINGS_CSV = path.join(DATA_DIR, "/listings.csv");

const FILE_ENCODING = "utf-8";

async function requireDataDir() {
  try {
    await access(DATA_DIR);
  } catch (err) {
    // Data dir does not exist
    console.log("Data dir does not exist. Creating", DATA_DIR);
    await mkdir(DATA_DIR);
    console.log("Data dir created.");
  }
}

async function getRunId() {
  await requireDataDir();
  try {
    const content = await readFile(LAST_RUN_FILE, { encoding: FILE_ENCODING });
    console.log("Last Run ID: ", content);
    const lastRun = parseInt(content);
    const runId = lastRun + 1;
    await updateLastRun(runId);
    return runId;
  } catch (err) {
    // No last run file.
    console.log("No last run. Defaulting to run ID = 1.");
    const runId = 1;
    await updateLastRun(runId);
    return runId;
  }
}

async function updateLastRun(lastRun) {
  const content = "" + lastRun;
  await writeFile(LAST_RUN_FILE, content, {
    encoding: FILE_ENCODING,
    flag: "w",
  });
}

function attachLogging(page) {
  page.on("console", (consoleObj) => console.log(consoleObj.text()));
}

async function getText(page, $el) {
  if (!$el) {
    return null;
  }

  return page.evaluate((node) => node.innerText, $el);
}

async function getAttribute(page, $el, attribute) {
  if (!$el) {
    return null;
  }

  return page.evaluate(
    (node, attribute) => node.getAttribute(attribute),
    $el,
    attribute
  );
}

function parseHousingInfo(housingText) {
  if (!housingText) {
    return {};
  }

  const parts = housingText.split(" - ").map((part) => part.trim());
  const brRegex = /^(\d+)br/i;
  const brMatch = parts[0].match(brRegex);

  const sqftRegex = /^(\d+)ft2/i;
  const sqftMatch = parts[1].match(sqftRegex);

  return {
    bedrooms: brMatch ? brMatch[1] : null,
    floorSqft: sqftMatch ? sqftMatch[1] : null,
  };
}

async function* parseListings(page) {
  // Get all results
  const resultRowSelector = "ul#search-results > li.result-row";
  await page.waitForSelector(resultRowSelector);
  const resultRows = await page.$$(resultRowSelector);

  for (const $resultRow of resultRows) {
    const postId = await getAttribute(page, $resultRow, "data-pid");

    const $resultInfo = await $resultRow.$("div.result-info");

    const $resultDate = await $resultInfo.$("time.result-date");
    const posted = await getAttribute(page, $resultDate, "datetime");

    const $headingLink = await $resultInfo.$(".result-heading > a");
    const title = await getText(page, $headingLink);
    const url = await getAttribute(page, $headingLink, "href");

    const $resultMeta = await $resultInfo.$(".result-meta");

    const $resultPrice = await $resultMeta.$(".result-price");
    const price = await getText(page, $resultPrice);

    const $housing = await $resultMeta.$(".housing");
    const housingText = await getText(page, $housing);
    const { bedrooms, floorSqft } = parseHousingInfo(housingText);

    const result = {
      postId,
      title,
      posted,
      url,
      price,
      bedrooms,
      floorSqft,
    };
    console.log(JSON.stringify(result));
    yield result;
  }
}

async function main() {
  // Increment the last run ID before doing anything else. It's very not good if two runs execute with the same value.
  const runId = await getRunId();
  console.log("Current run: ", runId);

  const browser = await puppeteer.launch({
    headless: false,
    slowMo: 10,
  });
  const page = await browser.newPage();
  attachLogging(page);

  await page.goto(START_URL, {
    waitUntil: "networkidle2",
  });

  const listingsStream = Readable.from(parseListings(page), {
    objectMode: true,
  });

  const csvTransform = new Transform({
    objectMode: true,
    transform(listing, encoding, callback) {
      const { postId, title, posted, url, price, bedrooms, floorSqft } =
        listing;
      const csvRow = [
        runId,
        postId,
        title,
        posted,
        url,
        price,
        bedrooms,
        floorSqft,
      ];
      callback(undefined, csvRow);
    },
  });

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

  const csvStringifier = stringify();

  console.log("Opening for append: ", RESULTS_CSV);
  const outfile = await open(RESULTS_CSV, "a");
  const writeStream = outfile.createWriteStream();
  writeStream.on("finish", () => {
    console.log("Closing output file...");
    outfile.close();
  });

  const finished = new Promise((resolve) => {
    pipeline(
      listingsStream,
      csvTransform,
      csvStringifier,
      loggingTransform,
      writeStream,
      resolve
    );
  });

  await finished;
  // await browser.close();
  console.log("Done.");
}

main();
