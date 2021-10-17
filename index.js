import puppeteer from "puppeteer";
import { Writable } from "stream";

import { crawlPosts } from "./crawl-posts";
import { fetchResults } from "./fetch-listings";
import { readListingsStream } from "./listings";
import { readResultsStream } from "./results";

const MILLIS_PER_MINUTE = 60 * 1000;
const FETCH_RESULTS_PERIOD = 5 * MILLIS_PER_MINUTE;
const RECRAWL_PERIOD = 60 * MILLIS_PER_MINUTE;
const NEW_POSTS_RECHECK_DELAY = 10 * 1000;

const START_URL =
  "https://vancouver.craigslist.org/d/apartments-housing-for-rent/search/apa";

const foundPosts = {};

function recordFoundPost(post) {
  const { postId } = post;
  if (foundPosts[postId]) {
    // Previously found
    return;
  }

  foundPosts[postId] = post;
}

function getPostUrl(postId) {
  const { url } = foundPosts[postId];
  return url;
}

function markPostFetched(postId, fetchedDate) {
  if (!foundPosts[postId]) {
    // Should never happen?
    throw new Error("Tried to update post which was never found: " + postId);
  }

  if (
    fetchedDate &&
    foundPosts[postId].lastCrawled &&
    fetchedDate.getTime() < foundPosts[postId].lastCrawled
  ) {
    // Ignore if we're trying to update to a date that is older than the current record.
    return;
  }

  // Set a last crawled date of now.
  foundPosts[postId].lastCrawled = fetchedDate || new Date();
}

function markPostRemoved(postId, removed) {
  if (!foundPosts[postId]) {
    // Should never happen?
    throw new Error("Tried to update post which was never found: " + postId);
  }

  // Set a last crawled date of now.
  foundPosts[postId].removed = removed;
}

function findNextPostsToCrawl() {
  const postIds = Object.keys(foundPosts);

  // Find the next post which has never been crawled.
  const nextUncrawledIds = postIds.filter(
    (postId) => !foundPosts[postId].lastCrawled
  );
  if (nextUncrawledIds.length > 0) {
    console.log("Found uncrawled %d posts.", nextUncrawledIds.length);
    return nextUncrawledIds;
  }

  // If every post has been crawled alread, then find any which are due to be recrawled.
  const nowMillis = Date.now();
  const overduePostIds = postIds.filter((postId) => {
    const { lastCrawled, removed } = foundPosts[postId];
    if (removed) {
      return false;
    }

    const elapsedMillis = nowMillis - lastCrawled.getTime();

    return elapsedMillis > RECRAWL_PERIOD;
  });
  console.log("Found %d overdue posts.", overduePostIds.length);

  return overduePostIds;
}

async function startFetchingListings(browser, onFindPost) {
  console.log("Fetching search results...");
  await fetchResults(browser, START_URL, onFindPost);

  console.log("Scheduling next listings fetch...");
  // Schedule another crawl. Don't resolve promise until all crawls complete.
  // Current design is we recurse indefinitely. This implies the promise will never resolve unless there's an error.
  return new Promise((resolve, reject) => {
    setTimeout(
      () =>
        startFetchingListings(browser, onFindPost) //
          .then(resolve)
          .catch(reject),
      FETCH_RESULTS_PERIOD
    );
  });
}

async function startCrawlingPosts(browser) {
  // Find the next posts to fetch
  const nextPostIds = findNextPostsToCrawl();
  console.log("Found %d posts ready to crawl.", nextPostIds.length);
  if (nextPostIds.length === 0) {
    // Wait a second and try again
    return new Promise((resolve, reject) =>
      setTimeout(
        () =>
          startCrawlingPosts(browser) //
            .then(resolve)
            .catch(reject),
        NEW_POSTS_RECHECK_DELAY
      )
    );
  }

  // Create a map of post URLs
  const postUrlsById = nextPostIds.reduce(
    (prev, postId) => ({
      ...prev,
      [postId]: getPostUrl(postId),
    }),
    {}
  );

  const handlePostCrawled = ({ postId, removed }) => {
    console.log("Crawled post: ", postId);
    markPostFetched(postId);
    if (removed) {
      console.log("Post was removed: ", postId);
      markPostRemoved(postId, removed);
    }
  };

  // Crawl the posts
  console.log("Crawling posts...");
  await crawlPosts(browser, nextPostIds, postUrlsById, handlePostCrawled);

  // Crawl the next post
  await startCrawlingPosts(browser);
}

async function hydrateFoundPosts() {
  // Load all results and build a map
  const results = await readResultsStream();

  const addFoundPostsWritable = new Writable({
    objectMode: true,
    write(result, encoding, callback) {
      recordFoundPost(result);
      callback();
    },
  });

  results.pipe(addFoundPostsWritable);

  // Iterate over all listings and extract lastFetch and removed info
  const listings = await readListingsStream();

  const updatePostsWritable = new Writable({
    objectMode: true,
    write(post, encoding, callback) {
      recordFoundPost(post);
      const { postId, removed, fetched } = post;
      if (fetched) {
        const fetchedTimestamp = new Date(Date.parse(fetched));
        markPostFetched(postId, fetchedTimestamp);
      }
      if (removed) {
        markPostRemoved(postId, removed);
      }
      callback();
    },
  });

  listings.pipe(updatePostsWritable);
}

async function main() {
  await hydrateFoundPosts();

  const browser = await puppeteer.launch({
    headless: false,
    slowMo: 10,
  });

  const handlePostFound = (post) => {
    console.log("Found post ", post.postId);
    recordFoundPost(post);
  };

  console.log("Starting fetch listings...");
  const doneFetchingListings = startFetchingListings(browser, handlePostFound);
  console.log("Starting crawl posts...");
  const doneCrawlingPosts = startCrawlingPosts(browser);

  console.log("Waiting for main tasks to complete...");
  await Promise.all([doneFetchingListings, doneCrawlingPosts]);
  // Probably never get here...
  await browser.close();
  console.log("Done.");
}

main();
