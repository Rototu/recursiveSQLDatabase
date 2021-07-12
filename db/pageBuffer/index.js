const { cacheOptions } = require('../../config');
// eslint-disable-next-line no-unused-vars
const Page = require('../page');
const LRU = require('lru-cache');
const { wait } = require('../../utilities');
const { pageFetchTime } = require('../../config');

/**
 * Buffer to hold a fixed amount of pages
 * A simulation of real-life page buffers, with a few notable differences:
 * 1. The db stores pages are in memory and a fixed penalty time is incurred
 * when fetching the iterator for a page based on the value in the config file
 * to simulate physical storage access times.
 * 2. The buffer holds iterators for the records in pages, not the pages themselves.
 * 3. Since all operations are expected to be executes synchronously and the record
 * iterator to be consumed in one go, page pinning is not implemented.
 */
class PageBuffer {
  constructor() {
    this._maxPages = cacheOptions.max;
    /** @type {LRU<string, Page} */
    this._cache = new LRU(cacheOptions);
  }

  /**
   * Adds a page to the buffer.
   * Automatically removes last recently used pages if needed.
   * @param {Page} page
   */
  addPage(page) {
    wait(pageFetchTime);
    this._cache.set(page.id, page);
  }

  /**
   * Test if page is in buffer without updating position in cache.
   * @param {string} pageId Id of page
   * @returns {boolean}
   */
  hasPage(pageId) {
    return this._cache.peek(pageId) ? true : false;
  }

  /**
   * Returns stored iterator in buffer. Updates position in LRU cache.
   * @param {string} pageId Id of page
   * @returns  {Generator}
   */
  getPageContents(pageId) {
    return this._cache.get(pageId).iterator();
  }

  /**
   * Empties page
   * @param {string} pageId Id of page
   */
  clearPage(pageId) {
    this._cache.get(pageId).clear();
  }

  /**
   * Inserts record into page
   * @param {*} pageId Id of page
   * @param {*} record Record
   * @return {number} Index of record
   */
  insertRecord(pageId, record) {
    const insertionResult = this._cache.get(pageId).insertRecord(record);
    if (insertionResult === false) {
      throw new Error("Cannot insert record, page doesn't have anymore space");
    }
    return insertionResult;
  }

  /**
   * Get record by index
   * @param {string} pageId Id of page
   * @param {number} index Record index
   * @returns {any}
   */
  getPageRecord(pageId, index) {
    return this._cache.get(pageId).directAccess(index);
  }

  /**
   * Empties the buffer/cache
   */
  clear() {
    this._cache.reset();
  }
}

module.exports = PageBuffer;
