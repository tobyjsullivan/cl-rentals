import { readFile, writeFile } from "fs/promises";
import stream, { Readable, Transform } from "stream";
import { promisify } from "util";
import { result as postResult } from "./results";

const FILE_ENCODING = "utf-8";

const pipeline = promisify(stream.pipeline);

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

async function* parseListings(runId, page) {
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

    const result = postResult(
      runId,
      postId,
      title,
      posted,
      url,
      price,
      bedrooms,
      floorSqft
    );
    console.log(JSON.stringify(result));
    yield result;
  }
}

export class FetchListings {
  constructor(resultSvc, lastRunFile) {
    this.resultSvc = resultSvc;
    this.lastRunFile = lastRunFile;
  }

  async fetchResults(browser, searchPageUrl, onFindPost) {
    // Increment the last run ID before doing anything else. It's very not good if two runs execute with the same value.
    const runId = await this._getRunId();
    console.log("Current run: ", runId);

    const page = await browser.newPage();
    try {
      attachLogging(page);

      await page.goto(searchPageUrl, {
        waitUntil: "networkidle2",
      });

      const listingsStream = Readable.from(parseListings(runId, page), {
        objectMode: true,
      });

      // Fires the onFindPost callback for every result found.
      const callbackTransform = new Transform({
        objectMode: true,
        transform(data, encoding, callback) {
          if (onFindPost && typeof onFindPost === "function") {
            onFindPost({ ...data });
          }
          callback(undefined, data);
        },
      });

      const writeStream = await this.resultSvc.writeResultsStream();

      await pipeline(listingsStream, callbackTransform, writeStream);
    } catch (err) {
      console.error("Error in fetchResults: ", err);
    } finally {
      try {
        await page.close();
      } catch (err) {
        console.warn("Error closing page in fetchResults: ", err);
      }
    }
  }

  async _getRunId() {
    try {
      const content = await readFile(this.lastRunFile, {
        encoding: FILE_ENCODING,
      });
      console.log("Last Run ID: ", content);
      const lastRun = parseInt(content);
      const runId = lastRun + 1;
      await this._updateLastRun(runId);
      return runId;
    } catch (err) {
      // No last run file.
      console.log("No last run. Defaulting to run ID = 1.");
      const runId = 1;
      await this._updateLastRun(runId);
      return runId;
    }
  }

  async _updateLastRun(lastRun) {
    const content = `${lastRun}`;
    await writeFile(this.lastRunFile, content, {
      encoding: FILE_ENCODING,
      flag: "w",
    });
  }
}
