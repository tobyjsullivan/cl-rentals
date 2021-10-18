import { access, mkdir } from "fs/promises";
import path from "path";
import puppeteer from "puppeteer";
import stream, { Writable } from "stream";
import { promisify } from "util";

import { crawlPosts } from "./crawl-posts";
import { FetchListings } from "./fetch-listings";
import { Listings } from "./listings";
import { Results } from "./results";

const MILLIS_PER_MINUTE = 60 * 1000;
const FETCH_RESULTS_PERIOD = 5 * MILLIS_PER_MINUTE;
const RECRAWL_PERIOD = 3 * 60 * MILLIS_PER_MINUTE;
const NEW_POSTS_RECHECK_DELAY = 10 * 1000;

const START_URL =
  "https://vancouver.craigslist.org/d/apartments-housing-for-rent/search/apa";

const DATA_DIR = process.env.DATA_DIR || "./data";
const LAST_RUN_FILE = path.join(DATA_DIR, "/last_run");
const LISTINGS_CSV = path.join(DATA_DIR, "/listings.csv");
const RESULTS_CSV = path.join(DATA_DIR, "/results.csv");

const foundPosts = {};

const pipeline = promisify(stream.pipeline);

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

async function startFetchingListings(browser, fetchListingSvc, onFindPost) {
  console.log("Fetching search results...");
  try {
    await fetchListingSvc.fetchResults(browser, START_URL, onFindPost);
  } catch (err) {
    console.error("Error during fetchResults: ", err);
  }

  console.log("Scheduling next listings fetch...");
  // Schedule another crawl. Don't resolve promise until all crawls complete.
  // Current design is we recurse indefinitely. This implies the promise will never resolve unless there's an error.
  return new Promise((resolve, reject) => {
    setTimeout(
      () =>
        startFetchingListings(browser, fetchListingSvc, onFindPost) //
          .then(resolve)
          .catch(reject),
      FETCH_RESULTS_PERIOD
    );
  });
}

async function startCrawlingPosts(browser, listingSvc) {
  // Find the next N posts to fetch
  const nextPostIds = findNextPostsToCrawl().slice(0, 10);
  console.log("Found %d posts ready to crawl.", nextPostIds.length);
  if (nextPostIds.length === 0) {
    // Wait a second and try again
    return new Promise((resolve, reject) =>
      setTimeout(
        () =>
          startCrawlingPosts(browser, listingSvc) //
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
  try {
    await crawlPosts(
      browser,
      listingSvc,
      nextPostIds,
      postUrlsById,
      handlePostCrawled
    );
  } catch (err) {
    console.error("Error during crawlPosts: ", err);
  }

  // Crawl the next post
  await startCrawlingPosts(browser, listingSvc);
}

async function hydrateFoundPosts(listingSvc, resultSvc) {
  // Load all results and build a map
  const results = await resultSvc.readResultsStream();

  const addFoundPostsWritable = new Writable({
    objectMode: true,
    write(result, encoding, callback) {
      recordFoundPost(result);
      callback();
    },
  });

  await pipeline(results, addFoundPostsWritable);

  // Iterate over all listings and extract lastFetch and removed info
  const listings = await listingSvc.readListingsStream();

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

  await pipeline(listings, updatePostsWritable);
}

async function main() {
  await requireDataDir();

  const listingSvc = new Listings(LISTINGS_CSV);
  const resultSvc = new Results(RESULTS_CSV);
  const fetchListingSvc = new FetchListings(resultSvc, LAST_RUN_FILE);

  await hydrateFoundPosts(listingSvc, resultSvc);
  const totalCount = Object.keys(foundPosts).length;
  console.log("Rehydrated %d found posts", totalCount);

  const browser = await puppeteer.launch({
    headless: true,
    slowMo: 10,
  });

  const handlePostFound = (post) => {
    console.log("Found post ", post.postId);
    recordFoundPost(post);
  };

  console.log("Starting fetch listings...");
  const doneFetchingListings = startFetchingListings(
    browser,
    fetchListingSvc,
    handlePostFound
  );
  console.log("Starting crawl posts...");
  const doneCrawlingPosts = startCrawlingPosts(browser, listingSvc);

  console.log("Waiting for main tasks to complete...");
  await Promise.all([doneFetchingListings, doneCrawlingPosts]);
  // Probably never get here...
  await browser.close();
  console.log("Done.");
}

main();
