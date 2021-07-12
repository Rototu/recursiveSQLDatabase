const { maxNoOfRecsPerFile } = require('../../config');

class Page {
  /**
   * Construct a new page
   * @param {string} pageId Page ID
   */
  constructor(pageId) {
    this._maxNoOfRecords = maxNoOfRecsPerFile;
    /** @type {any[]}*/
    this._files = new Array();
    this._id = pageId;
  }

  /**
   * Page ID
   * @returns {string} Page ID
   */
  get id() {
    return this._id;
  }

  /**
   * Get a record if you know the index
   * @param {number} index Record index
   * @returns Record copy
   */
  directAccess(index) {
    if (index >= this._files.length)
      throw new Error('Record index out of bounds');
    return Object.assign({}, this._files[index]);
  }

  /**
   * Fresh iterator for page values
   * @returns {Generator}
   */
  *iterator() {
    yield* this._files.slice();
  }

  /**
   * Inserts a record in the page
   * @param {any} record Record to insert
   * @returns {boolean|number} Record index if record inserted successfully, false otherwise
   */
  insertRecord(record) {
    if (this._files.length < this._maxNoOfRecords) {
      this._files.push(record);
      return this._files.length - 1;
    }
    return false;
  }

  /**
   * How many free spaces are left in page
   * @returns {number}
   */
  get spacesLeft() {
    return this._maxNoOfRecords - this._files.length;
  }

  /**
   * Empty page
   */
  clear() {
    this._files = new Array();
  }
}

module.exports = Page;
