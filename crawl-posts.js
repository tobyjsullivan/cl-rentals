import puppeteer from "puppeteer";
import { pipeline, Readable, Transform } from "stream";
import { listing, readListingsStream, writeListingsStream } from "./listings";
import { readResultsStream } from "./results";

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

async function crawlPosts(browser, postIds, postUrlsById) {
  const streamPostIds = Readable.from(postIds);

  // Waits a short time between each chunk to slow crawl and avaid hitting rate limits.
  let nextDelayMs = 500;
  const slowMoTransform = new Transform({
    objectMode: true,
    transform(data, encoding, callback) {
      const delayMs = nextDelayMs;
      nextDelayMs += 500;
      setTimeout(() => {
        nextDelayMs -= 500;
        callback(undefined, data);
      }, delayMs);
    },
  });

  const crawlTransform = new Transform({
    objectMode: true,
    async transform(data, encoding, callback) {
      try {
        const postId = data;
        console.log("Received Post ID: ", postId);
        const url = postUrlsById[postId];
        const crawlResult = await crawlPost(browser, postId, url);
        console.log("crawlResult: ", crawlResult);
        callback(undefined, crawlResult);
      } catch (err) {
        console.log("Crawl error: ", err);
        callback(err);
      }
    },
  });

  return streamPostIds //
    .pipe(slowMoTransform) //
    .pipe(crawlTransform);
}

function parseHousingText(housingText) {
  const bedroomRegex = /(\d+)br/i;
  const bedroomMatch = housingText.match(bedroomRegex);

  const sqftRegex = /(\d+)ft2/i;
  const sqftMatch = housingText.match(sqftRegex);

  const bedrooms = bedroomMatch ? parseInt(bedroomMatch[1]) : null;
  const sqft = sqftMatch ? parseInt(sqftMatch[1]) : null;

  return {
    bedrooms,
    sqft,
  };
}

async function crawlPost(browser, postId, url) {
  const page = await browser.newPage();
  try {
    attachLogging(page);
    await page.goto(url, {
      waitUntil: "networkidle2",
    });

    const bodySelector = ".body";
    await page.waitForSelector(bodySelector);
    const $body = await page.$(bodySelector);

    const $removed = await $body.$(".removed");
    if ($removed) {
      const removedText = await getText(page, $removed);

      console.log("Post", postId, "has been removed");
      return listing(
        postId,
        url,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        new Date().toISOString(),
        removedText
      );
    }

    const $title = await $body.$(".postingtitletext");
    const $price = await $title.$(".price");
    const price = $price ? await getText(page, $price) : null;

    const $housing = await $title.$(".housing");
    const housingText = $housing ? await getText(page, $housing) : null;
    const housingInfo = housingText ? parseHousingText(housingText) : null;
    const { bedrooms, sqft } = housingInfo || {};

    const $titleText = await $title.$(".titletextonly");
    const titleText = await getText(page, $titleText);

    const $userBody = await $body.$(".userbody");

    const $map = await $userBody.$("#map");
    const latitude = await getAttribute(page, $map, "data-latitude");
    const longitude = await getAttribute(page, $map, "data-longitude");
    const accuracy = await getAttribute(page, $map, "data-accuracy");

    const $mapAddress = await $map.$(".mapaddress");
    const streetAddress = $mapAddress ? await getText(page, $mapAddress) : null;

    const $postBody = await $userBody.$("#postingbody");
    const description = await getText(page, $postBody);

    const $postInfos = await $userBody.$(".postinginfos");
    const $postInfoTimes = await $postInfos.$$("p > time");
    const $postedTime = $postInfoTimes.length > 0 ? $postInfoTimes[0] : null;
    const posted = await getAttribute(page, $postedTime, "datetime");
    const $updatedTime = $postInfoTimes.length > 1 ? $postInfoTimes[1] : null;
    const updated = $updatedTime
      ? await getAttribute(page, $updatedTime, "datetime")
      : null;

    const result = listing(
      postId,
      url,
      price,
      bedrooms,
      sqft,
      latitude,
      longitude,
      accuracy,
      streetAddress,
      titleText,
      description,
      posted,
      updated,
      null,
      null
    );
    console.log("Parsed listing: ", result);

    return result;
  } finally {
    page.close();
  }
}

async function main() {
  const browser = await puppeteer.launch({
    headless: true,
    slowMo: 80,
  });

  // Read CSV of listings and build map of [Post ID => URL]
  const postUrlsById = {};
  for await (const { postId, url } of await readResultsStream()) {
    postUrlsById[postId] = url;
  }

  // Create a list of distinct Post IDs
  const allPostIds = Object.keys(postUrlsById);

  // Read CSV of previously crawled posts and create a list of all existing Post IDs
  const existingPostIds = [];
  for await (const { postId } of await readListingsStream()) {
    existingPostIds.push(postId);
  }

  // Find the set of Post IDs which have not been crawled yet.
  const missingPostIds = allPostIds.filter(
    (id) => !existingPostIds.includes(id)
  );

  console.log("Planning to crawl %d posts.", missingPostIds.length);

  // Crawl all the remaining posts
  const listingStream = await crawlPosts(browser, missingPostIds, postUrlsById);

  const writeStream = await writeListingsStream();

  pipeline(listingStream, writeStream, async () => {
    console.log("Done.");
    await browser.close();
  });
}

main();