const PageBuffer = require('./pageBuffer');
const Page = require('./page');
const HashMap = require('hashmap');
const PriorityQueue = require('fastpriorityqueue');
const { nanoid } = require('nanoid');
const chalk = require('chalk');
const { maxNoOfRecsPerFile, blockJoinSize } = require('../config');

/**
 * DB singleton class
 */
class DB {
  constructor() {
    // Went overboard with the hashmaps here, probably
    // could have used regular JS objects as dictionaries
    // but I enjoy the clear interface of the hashmap

    /** DB page buffer */
    this._buf = new PageBuffer();
    /**
     * Hashmap from table names to a table
     * (which itself is a hashmap of pageIds to pages)
     * @type {HashMap<string, HashMap<string, Page>}
     */
    this._tables = new HashMap();
    /**
     * Hashmap from table names to priority queues
     * telling us which are the most free pages
     * @type {HashMap<string, PriorityQueue<{pageId: string, spacesLeft: number}>>}
     */
    this._tablePageSpaces = new HashMap();
    /**
     * Hashmap from table names to an array of column names
     * @type {HashMap<string, string[]>}
     */
    this._tableCols = new HashMap();
    /**
     * Hashmap from tableName to a hashmap of colnames to
     * the actual hashIndex on that col for the table
     * @type {HashMap<string, HashMap<string, HashMap<string|number, [[string, number]]>>>}
     */
    this._tableHashes = new HashMap();
  }

  /**
   * Comparator for internal priority queue
   * @param {{pageId: string, spacesLeft: number}} entry1
   * @param {{pageId: string, spacesLeft: number}} entry2
   * @returns {boolean} Is entry1 > entry2
   */
  static _pageSpaceComparator(entry1, entry2) {
    return entry1.spacesLeft > entry2.spacesLeft;
  }

  /**
   * Drops all tables in the db
   */
  dropAllTables() {
    for (const table of this._tables.keys()) {
      this.drop(table);
    }
    // No really necessary but why not
    this._buf = new PageBuffer();
  }

  /**
   * Create a new table in the db with one empty page
   * @param {string} tableName Name of table to create
   * @param {string[]} colNames Column keys for table
   * @returns {boolean} True if creation successfull, false if table already exists
   */
  addTable(tableName, colNames) {
    if (this._tables.keys().includes(tableName)) return false;

    const table = new HashMap();
    this._tables.set(tableName, table);
    this._tableHashes.set(tableName, new HashMap());

    const tablePageQueue = new PriorityQueue(DB._pageSpaceComparator);
    this._tablePageSpaces.set(tableName, tablePageQueue);

    this._tableCols.set(tableName, colNames.slice());

    this._addNewPageToTable(tableName);

    return true;
  }

  /**
   * Adds a new empty page to a table
   * @param {string} tableName Table to add page to
   */
  _addNewPageToTable(tableName) {
    const id = nanoid();
    const page = new Page(id);

    this._tables.get(tableName).set(id, page);
    this._tablePageSpaces
      .get(tableName)
      .add({ pageId: id, spacesLeft: page.spacesLeft });
  }

  /**
   *
   * @param {string} tableName Table name
   * @returns {{pageId: string, spacesLeft: number}}
   */
  _getPageToInsert(tableName) {
    const queue = this._tablePageSpaces.get(tableName);
    const shouldCreateNewPage = queue.peek().spacesLeft === 0;
    if (shouldCreateNewPage) {
      this._addNewPageToTable(tableName);
    }
    return queue.poll();
  }

  /**
   * Inserts records into given table
   * @param {string} tableName Table Name
   * @param {any[]} records Array of records to insert
   */
  insertRecords(tableName, records) {
    const recordsCopy = records.slice();
    const table = this._tables.get(tableName);
    const queue = this._tablePageSpaces.get(tableName);

    while (recordsCopy.length > 0) {
      const { pageId, spacesLeft } = this._getPageToInsert(tableName);
      const page = table.get(pageId);
      if (!this._buf.hasPage(pageId)) {
        this._buf.addPage(page);
      }

      for (let i = 0; i < spacesLeft && recordsCopy.length > 0; i++) {
        let record = Object.assign({}, recordsCopy.shift());

        if (!('_id' in record)) {
          // add id if record lacks one
          const recordId = `${tableName}:${nanoid()}`;
          record = { _id: recordId, ...record };
        }

        this._buf.insertRecord(pageId, record);
      }

      queue.add({ pageId: pageId, spacesLeft: page.spacesLeft });
    }
  }

  /**
   * Insert records while making sure no records with duplicate ids are inserted
   * @param {any[]} records
   * @param {string} tableName
   */
  insertUniqueRecordsById(tableName, records) {
    let recs = records.slice();
    const table = this._tables.get(tableName);
    const queue = this._tablePageSpaces.get(tableName);

    if (!this.isTableHashed(tableName, '_id')) {
      this.hashTable(tableName, '_id', true);
    }
    const hashIndex = this._tableHashes.get(tableName).get('_id');
    recs.forEach((rec) => {
      const { _id } = rec;
      if (hashIndex.has(_id)) {
        return;
      } else {
        const { pageId } = this._getPageToInsert(tableName);
        const page = table.get(pageId);
        if (!this._buf.hasPage(pageId)) {
          this._buf.addPage(page);
        }
        const index = this._buf.insertRecord(pageId, rec);
        queue.add({ pageId: pageId, spacesLeft: page.spacesLeft });
        hashIndex.set(_id, [[pageId, index]]);
      }
    });
    // this.insertRecords(tableName, recs);
  }

  /**
   * Empties a table
   * @param {string} tableName Table name
   */
  clearTable(tableName) {
    const table = this._tables.get(tableName);
    const pages = table.values();
    const newTableSpaces = new Array();
    pages.forEach((page) => {
      if (!this._buf.hasPage(page.id)) {
        !this._buf.addPage(page);
      }
      this._buf.clearPage(page.id);
      newTableSpaces.push({ pageId: page.id, spacesLeft: page.spacesLeft });
    });
    this._tablePageSpaces.get(tableName).heapify(newTableSpaces);
    this._tableHashes.set(tableName, new HashMap());
  }

  /**
   * Get an iterator for all records in the given table
   * @param {string} tableName Table name to fetc records from
   * @returns {Generator} Generator to iterate through all records in table
   */
  *getAllRecords(tableName) {
    const table = this._tables.get(tableName);
    const buf = this._buf;

    for (const pageId of table.keys()) {
      if (!buf.hasPage(pageId)) {
        buf.addPage(table.get(pageId));
      }

      const pageIterator = buf.getPageContents(pageId);
      for (const record of pageIterator) {
        yield record;
      }
    }
  }

  /**
   * Block join on two generators
   * @param {string} t1 Table 1 Name
   * @param {string} t2 Table 2 Name
   * @param {{colNameDst: string, colSrc: [string, string]}[]} projection Projection (colSrc = [tName, cName])
   * @param {boolean} [shouldGenPairId=false] Add a composed id (id1+id2)
   * @returns {Generator}
   */
  *blockJoin(tableName1, tableName2, projection, shouldGenPairId) {
    let done = false,
      value;
    let block = [];
    const t1Generator = DB.getAllRecords(tableName1);
    while (!done) {
      ({ done, value } = t1Generator.next());
      block.push(value);
      if (block.length === blockJoinSize || done) {
        /** @type {Generator}*/
        const t2Generator = DB.getAllRecords(tableName2);
        for (const rec1 of block) {
          for (const rec2 of t2Generator) {
            const res = new Object();
            projection.forEach(({ colNameDst, colSrc }) => {
              const [srcTName, srcCName] = colSrc;
              switch (srcTName) {
                case tableName1:
                  res[colNameDst] = rec1[srcCName];
                  break;
                case tableName2:
                  res[colNameDst] = rec2[srcCName];
                  break;
                default:
                  throw new Error('Unexpected projection');
              }
            });
            if (shouldGenPairId) {
              res[`_id${tableName1}`] = rec1._id;
              res[`_id${tableName2}`] = rec2._id;
            }
            yield res;
          }
        }
        block = [];
      }
    }
  }

  /**
   * Performs a hash join on two tables.
   * @param {string} t1 Name of first table
   * @param {string} c1 Name of column to hash in table 1
   * @param {string} t2 Name of second table
   * @param {string} c2 Name of column to hash in table 2
   * @param {{colNameDst: string, colSrc: [string, string]}[]} projection Projection (colSrc = [tName, cName])
   * @param {string} op '>' | '='
   * @param {boolean} [shouldGenPairId=false] Add a composed id (id1+id2)
   * @returns {Generator}
   */
  *hashJoin(t1, c1, t2, c2, projection, op, shouldGenPairId = false) {
    // page tables
    const table1 = this._tables.get(t1);
    const table2 = this._tables.get(t2);

    // hash tables
    if (!this.isTableHashed(t1, c1)) this.hashTable(t1, c1, true);
    if (!this.isTableHashed(t2, c2)) this.hashTable(t2, c2, true);

    // get hash indexes
    const index1 = this._tableHashes.get(t1).get(c1);
    const index2 = this._tableHashes.get(t2).get(c2);

    // get values for first table
    const vals1 = index1.keys();

    // Hash join
    for (const val1 of vals1) {
      // get all records in t1 with c1 = val1
      const recs1 = [];
      for (const [pageId, index] of index1.get(val1)) {
        if (!this._buf.hasPage(pageId)) {
          this._buf.addPage(table1.get(pageId));
        }
        recs1.push(this._buf.getPageRecord(pageId, index));
      }
      // get values to keep from t2 based on op
      const vals2 = index2.keys().filter((val2) => {
        switch (op) {
          case '>':
            return val1 > val2;
          case '=':
            return val1 == val2; // we ignore type equality
          default:
            return false;
        }
      });
      // get records to join from table 2
      const recs2 = [];
      for (const val2 of vals2) {
        for (const [pageId, index] of index2.get(val2)) {
          if (!this._buf.hasPage(pageId)) {
            this._buf.addPage(table2.get(pageId));
          }
          recs2.push(this._buf.getPageRecord(pageId, index));
        }
      }
      // project && join
      for (const rec1 of recs1) {
        for (const rec2 of recs2) {
          const res = new Object();
          projection.forEach(({ colNameDst, colSrc }) => {
            const [srcTName, srcCName] = colSrc;
            switch (srcTName) {
              case t1:
                res[colNameDst] = rec1[srcCName];
                break;
              case t2:
                res[colNameDst] = rec2[srcCName];
                break;
              default:
                throw new Error('Unexpected projection');
            }
          });
          if (shouldGenPairId) {
            res._id = `${rec1._id}|${rec2._id}`; // composite id
            res[`_id${t1}`] = rec1._id;
            res[`_id${t2}`] = rec2._id;
          }
          yield res;
        }
      }
    }
  }

  /**
   * Test if a table has a hash index on a given column
   * @param {string} tableName Name of table
   * @param {string} col Name of column
   * @returns {boolean}
   */
  isTableHashed(tableName, col) {
    const hashedCols = this._tableHashes.get(tableName);
    return hashedCols.has(col);
  }

  /**
   * Get all records satisfying condition from table
   * @param {string} tableName Name of table
   * @param {string} colName Name of column
   * @param {string} op '=' | '>'
   * @param {number|string} rhs rhs to compare with
   * @returns {Generator}
   */
  *getRecsFromHash(tableName, colName, op, rhs) {
    if (!this.isTableHashed(tableName, colName))
      throw new Error('Table needs to be hashed first');
    const hashIndex = this._tableHashes.get(tableName).get(colName);
    switch (op) {
      case '=':
        if (!hashIndex.has(rhs)) return;
        for (const [pageId, index] of hashIndex.get(rhs)) {
          if (!this._buf.hasPage(pageId))
            this._buf.addPage(this._tables.get(tableName).get(pageId));
          yield this._buf.getPageRecord(pageId, index);
        }
        break;
      case '>':
        for (const val of hashIndex.keys().filter((key) => key > rhs)) {
          for (const [pageId, index] of hashIndex.get(val)) {
            if (!this._buf.hasPage(pageId))
              this._buf.addPage(this._tables.get(tableName).get(pageId));
            yield this._buf.getPageRecord(pageId, index);
          }
        }
        break;
      default:
        throw new Error('Unexpected operator');
    }
  }

  /**
   * Test if an indexed table has a value in the given col
   * @param {string} tableName Name of indexed table
   * @param {string} colName Name of column
   * @param {string|number} val Column value
   * @returns {boolean}
   */
  hasValue(tableName, colName, val) {
    if (!this.isTableHashed(tableName, colName)) {
      throw new Error('Table must be hashed first');
    }
    return this._tableHashes.get(tableName).get(colName).has(val);
  }

  /**
   * Drop a table if it exists
   * @param {string} tableName Name of table to drop
   */
  drop(tableName) {
    if (this._tables.has(tableName)) {
      this.clearTable(tableName);
      this._tables.delete(tableName);
      this._tablePageSpaces.delete(tableName);
      this._tableHashes.delete(tableName);
      this._tableCols.delete(tableName);
    }
  }

  /**
   * Creates a hashIndex on a column of the table. New entries will not be hashed!
   * @param {string} tableName Name of table
   * @param {string} colName Name of column to hash
   * @param {boolean} [isNew=false] Should clear old hashindex on table/col pair, default false
   */
  hashTable(tableName, colName, isNew = false) {
    /** @type {HashMap<string|number, [[string, number]]>} */
    const hashIndex = isNew
      ? new HashMap()
      : this._tableHashes.get(tableName).get(colName);
    const table = this._tables.get(tableName);
    const buf = this._buf;

    for (const pageId of table.keys()) {
      if (!buf.hasPage(pageId)) {
        buf.addPage(table.get(pageId));
      }

      const pageIterator = buf.getPageContents(pageId);

      let index = 0;
      for (const record of pageIterator) {
        const colVal = record[colName];
        if (!hashIndex.has(colVal)) {
          hashIndex.set(colVal, [[pageId, index]]);
        } else {
          hashIndex.get(colVal).push([pageId, index]);
        }
        index++;
      }
    }

    this._tableHashes.get(tableName).set(colName, hashIndex);
  }

  /**
   * Retrieve all records satisfying filter function
   * @param {string} tableName Name of table
   * @param {Function} filterFunction Filter function to apply to records
   */
  *filterRecords(tableName, filterFunction) {
    for (const record of this.getAllRecords(tableName)) {
      if (filterFunction(record)) {
        yield record;
      }
    }
  }

  /**
   *
   * @param {string} tableName Table name
   * @returns {string[]} Column names in table
   */
  getTableKeys(tableName) {
    return this._tableCols.get(tableName).slice();
  }

  /**
   * Get the number of entries in the table
   * @param {string} tableName Name of table
   * @returns {number}
   */
  getNumberOfEntries(tableName) {
    const noOfPages = this._tables.get(tableName).count();
    let recordsInTable = maxNoOfRecsPerFile * noOfPages;
    this._tablePageSpaces.get(tableName).forEach(({ spacesLeft }) => {
      recordsInTable -= spacesLeft;
    });
    return recordsInTable;
  }

  /**
   * Creates new table with the records in the original table in ascending
   * order of the given column vals and hashes it too
   * @param {string} tableName Name of table
   * @param {string} colName Name of column
   * @returns {string} Name of created table
   */
  copyIntoSortedTable(tableName, colName) {
    const table = this._tables.get(tableName);

    // create temporary table
    const sortedTableName = nanoid();
    this.addTable(sortedTableName, this.getTableKeys(tableName));

    // hash source table on col
    this.hashTable(tableName, colName, true);
    const hashIndex = this._tableHashes.get(tableName).get(colName);

    // get values in column in sorted order
    const sortedVals = hashIndex.keys().sort();

    // insert values in ascending order into temp table
    for (const val of sortedVals) {
      const recs = [];
      for (const [pageId, index] of hashIndex.get(val)) {
        if (!this._buf.hasPage(pageId)) {
          this._buf.addPage(table.get(pageId));
        }
        recs.push(this._buf.getPageRecord(pageId, index));
      }
      this.insertRecords(sortedTableName, recs);
    }
    return sortedTableName;
  }

  /**
   * Prints stats about all tables in the db
   */
  printStats() {
    console.info('\nTable stats:');
    const tableNames = this._tables.keys();
    tableNames.forEach((name) => {
      const noOfPages = this._tables.get(name).count();
      const recordsInTable = this.getNumberOfEntries(name);
      const spaceUsage = (
        (100 * recordsInTable) /
        (maxNoOfRecsPerFile * noOfPages)
      ).toFixed(2);
      console.info(
        `Table ${chalk.green(name)} has ${chalk.green(
          noOfPages
        )} pages, ${chalk.green(
          recordsInTable
        )} records, with a total occupancy of ${chalk.yellow(spaceUsage)}%.`
      );
    });

    console.info('\n');
  }
}

const SINGLETON_DB = new DB();

module.exports = SINGLETON_DB;
