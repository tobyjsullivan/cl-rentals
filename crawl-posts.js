import { pipeline, Readable, Transform } from "stream";
import { listing } from "./listings";

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

export async function crawlPosts(
  browser,
  listings,
  postIds,
  postUrlsById,
  onPostCrawled
) {
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
        callback(undefined, crawlResult);
      } catch (err) {
        console.log("Crawl error: ", err);
        callback(err);
      }
    },
  });

  const callbackTransform = new Transform({
    objectMode: true,
    transform(crawlResult, encoding, callback) {
      if (onPostCrawled && typeof onPostCrawled === "function") {
        onPostCrawled({ ...crawlResult });
      }
      callback(undefined, crawlResult);
    },
  });

  const writeStream = await listings.writeListingsStream();

  return new Promise((resolve) =>
    pipeline(
      streamPostIds,
      slowMoTransform,
      crawlTransform,
      callbackTransform,
      writeStream,
      resolve
    )
  );
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
  // console.log("crawlPost(", browser, ", ", postId, ", ", url, ")");
  const page = await browser.newPage();
  try {
    attachLogging(page);
    const response = await page.goto(url, {
      waitUntil: "networkidle2",
    });

    const status = response.status();
    if (status > 399) {
      console.log(`URL returned error status ${status}. URL: ${url}`);
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
        status <= 499 ? fetched.toISOString() : null,
        `URL returned error status ${status}.`,
        fetched.toISOString()
      );
    }

    const fetched = new Date();

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
        fetched.toISOString(),
        removedText,
        fetched.toISOString()
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

    const $mapAddress = await $userBody.$(".mapaddress");
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
      null,
      fetched.toISOString()
    );

    return result;
  } catch (err) {
    console.error("Error crawling post ", postId, url, err);
  } finally {
    try {
      await page.close();
    } catch (err) {
      console.warn("Failed to close page: ", err);
    }
  }
}
