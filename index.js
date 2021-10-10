const puppeteer = require('puppeteer');

const startUrl = 'https://vancouver.craigslist.org/d/apartments-housing-for-rent/search/apa';

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

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    slowMo: 100,
  });
  const page = await browser.newPage();
  attachLogging(page);

  await page.goto(startUrl, {
    waitUntil: "networkidle2",
  });

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
  }

  // await browser.close();
})();
