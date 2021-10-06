const puppeteer = require('puppeteer');

const startUrl = 'https://vancouver.craigslist.org/d/apartments-housing-for-rent/search/apa';

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    slowMo: 100,
  });
  const page = await browser.newPage();
  await page.goto(startUrl, {
    waitUntil: 'networkidle2',
  });

  // Get all results
  const resultRowSelector = 'ul#search-results > li.result-row';
  await page.waitForSelector(resultRowSelector);
  const resultRows = await page.$$(resultRowSelector);

  for (const resultRowEl of resultRows) {
    const postId = await page.evaluate((node) => node.getAttribute('data-pid'), resultRowEl);
    const resultEl = await resultRowEl.$('div.result-info');

    const timeEl = await resultEl.$('time.result-date');
    const timestamp = await page.evaluate((node) => node.getAttribute('datetime'), timeEl);
    
    const linkEl = await resultEl.$('h3.result-heading > a');
    const linkUrl = await page.evaluate((node) => node.getAttribute('href'), linkEl);

    const result = {
      pid: postId,
      timestamp,
      url: linkUrl,
    }
    console.log(`post: `, JSON.stringify(result));
  }

  // await browser.close();
})();
