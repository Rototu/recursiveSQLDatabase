/* eslint-disable no-unused-vars */

// libraries
const HashMap = require('hashmap');
const { nanoid } = require('nanoid');

// own code
const {
  Query,
  WithDecl,
  Term,
  Column,
  Operation,
} = require('../../query/classes');
const {
  zip2,
  eliminateObjectDuplicates,
  eliminateObjectKeys,
} = require('../../utilities');
const DB = require('../../db');

// for debugging purposes only
function printTableEntries(tableName) {
  for (const rec of DB.getAllRecords(tableName)) {
    console.log(rec);
  }
}

/**
 * Remove all _id properties from a record
 * @param {any} record Source record
 * @returns {any} Shallow copy of record with all _id properties removed
 */
function deleteIds(record) {
  const res = {};
  const recKeys = Object.keys(record);
  recKeys.forEach((key) => {
    if (!key.startsWith('_id')) {
      res[key] = record[key];
    }
  });
  return res;
}

/**
 * Make the json string of the record object the record's key.
 * @param {any} record Record
 * @returns {any} Modified record copy
 */
function addRecordAsId(record) {
  const res = Object.assign({}, record);
  res._id = JSON.stringify(record);
  return res;
}

/**
 * Get all columns of table that do not start with _id
 * @param {string} tName Table name
 * @returns {string[]} All columns of table that do not start with _id
 */
function getNonIdCols(tName) {
  DB.getTableKeys(tName).filter((colName) => !colName.startsWith('_id'));
}

/**
 * Construct the temporary working table
 * @param {WithDecl} withDecl Parsed WITH section of query
 * @returns {string} Name of table created
 */
function createRecursiveTable(withDecl) {
  const tableName = withDecl.name;
  const colNames = withDecl.columns;
  DB.addTable(tableName, colNames);
  return tableName;
}

/**
 * Executes a term of the query
 * @param {Term} term Term to execute
 * @param {string} tempTableName
 * @param {string} resultTableName
 * @returns {number} Number of results added to result table.
 */
function executeTerm(term, tempTableName, resultTableName) {
  let columns = term.cols;
  const initialNumberOfEntries = DB.getNumberOfEntries(resultTableName);

  // if SELECT * was used
  if (columns.length === 1 && columns[0].shouldFetchAllColumns) {
    const tableName = term.tables[0]; // should only be one table with SELECT *
    const options = {};
    let results = [];

    let filterFunction = (record) => true;

    if (term.ops) {
      filterFunction = constructFilter(term.ops, tableName);
    }

    DB.clearTable(tempTableName);
    for (const record of DB.filterRecords(tableName, filterFunction)) {
      const res = addRecordAsId(deleteIds(record));
      DB.insertUniqueRecordsById(tempTableName, [res]);
      DB.insertUniqueRecordsById(resultTableName, [res]);
    }
  } else {
    // SELECT * was not used
    /**
     * Map from table names to the columns we must keep from said tables
     * @type {Map<string, string[]>} */
    const groupedCols = new Map();

    columns.forEach((col) => {
      const { name, parentTable } = col;

      if (!(parentTable in groupedCols)) {
        groupedCols.set(parentTable, []);
      }

      groupedCols.get(parentTable).push(col);
    });

    // We do not want to modify original array by accident
    const tables = term.tables.slice();

    /**
     * Sort term operations into categories:
     *
     *  simpleOps:  list of operations between column and constant
     *
     *  compositeOps: list of operations between two columns
     *
     *  noOps: list of tables from which we just retrieve all entries
     * @returns {{simpleOps: Map<string, Operation[]>, compositeOps: Map<string, Operation[]>, noOps: string[]}}
     */
    const sortOps = () => {
      /** @type {Map<string, Operation[]>} */
      const simpleOps = new Map();
      /** @type {Map<string, Operation[]>} */
      const compositeOps = new Map();

      const tablesWithOps = new Set();
      if (term.ops) {
        term.ops.forEach((op) => {
          const { lhs, rhs, operator } = op;
          const lhsParentTable = lhs.parentTable;

          tablesWithOps.add(lhsParentTable);

          if (op.hasBothSidesCols()) {
            // composite json string id will help us retrieve data later
            const joinedId = JSON.stringify(
              [lhsParentTable, rhs.parentTable].sort()
            );

            if (!compositeOps.has(joinedId)) {
              compositeOps.set(joinedId, []);
            }

            compositeOps.get(joinedId).push(op);
            tablesWithOps.add(rhs.parentTable);
          } else {
            if (!simpleOps.has(lhsParentTable)) {
              simpleOps.set(lhsParentTable, []);
            }

            simpleOps.get(lhsParentTable).push(op);
          }
        });
      }

      const noOps = tables
        .slice()
        .filter((tableName) => !tablesWithOps.has(tableName));

      return {
        simpleOps,
        compositeOps,
        noOps,
      };
    };

    /**
     * Plans and executes term
     * @param {{simpleOps: Map<string, Operation[]>, compositeOps: Map<string, Operation[]>, noOps: string[]}} sortedOps
     */
    const execute = ({ simpleOps, compositeOps, noOps }) => {
      // First simplify tables from simpleOps
      const simpleTables = simpleOps.keys();
      /**
       * Map telling us if we should use original table or a simplified one later on
       * @type {Map<string, string>} */
      const tableNameMap = new Map(zip2(tables, tables));
      for (const tName of simpleTables) {
        // We will merge the results of all simple where operations into this simplified table
        const simplifiedTableId = nanoid();
        DB.addTable(simplifiedTableId, DB.getTableKeys(tName));
        tableNameMap.set(tName, simplifiedTableId);

        const tempTables = [];

        const ops = simpleOps.get(tName);
        for (const op of ops) {
          // Temporary table to hold data before join into simplifiedTableId
          const tTableName = nanoid();
          tempTables.push(tTableName);
          DB.addTable(tTableName, DB.getTableKeys(tName));

          const {
            lhs: { name: cName },
            rhs,
            operator,
          } = op;

          // Hash table if needed
          if (!DB.isTableHashed(tName, cName)) {
            DB.hashTable(tName, cName, true);
          }

          // Get records satisfying where condition and put them into a temporary table
          const iterator = DB.getRecsFromHash(tName, cName, operator, rhs);
          for (const rec of iterator) {
            DB.insertUniqueRecordsById(tTableName, [rec]);
          }
        }
        // If we have at least two temporary tables, we join them into one
        if (tempTables.length >= 2) {
          const firstTempTableName = tempTables[0];
          const remainingTables = tempTables.slice(1);

          for (const rec of DB.getAllRecords(firstTempTableName)) {
            // Test if rec is in all the other temporary tables
            // This is O(n) only since all temp tables already have a hash index on _id
            if (
              remainingTables.every((rtName) =>
                DB.hasValue(rtName, '_id', rec._id)
              )
            ) {
              DB.insertUniqueRecordsById(simplifiedTableId, [rec]);
            }
          }
        } else {
          // If only one table, just copy it
          for (const rec of DB.getAllRecords(tempTables[0])) {
            DB.insertUniqueRecordsById(simplifiedTableId, [rec]);
          }
        }

        for (const tTable of tempTables) {
          DB.drop(tTable);
        }
      }

      // Now we perform composite indexed joins from two tables

      // We first build the projection array needed for the DB.hashJoin method
      const proj = DB.getTableKeys(tempTableName).map((tcName, index) => {
        const c = columns[index];
        return {
          colNameDst: tcName,
          colSrc: [c.parentTable, c.name],
        };
      });

      // for every pair of tables that has WHERE ops declared
      for (const compositeId of compositeOps.keys()) {
        const pairOps = compositeOps.get(compositeId);
        // we will again store the results in temporary tables
        let tempNames = [];

        // for every composite operation
        for (const pairOp of pairOps) {
          const { rhs, lhs, operator } = pairOp;

          // create temp table
          const tTableName = nanoid();
          tempNames.push(tTableName);
          DB.addTable(tTableName, [
            ...groupedCols.get(lhs.parentTable),
            ...groupedCols.get(rhs.parentTable),
          ]);

          // filter projection list to only the columns from the tables we are processing now
          const tProj = proj.filter((p) =>
            [lhs.parentTable, rhs.parentTable].includes(p.colSrc[0])
          );

          const src1 = tableNameMap.get(lhs.parentTable);
          const src2 = tableNameMap.get(rhs.parentTable);

          // Iterator for hash joined elements
          const joinedIterator = DB.hashJoin(
            src1,
            lhs.name,
            src2,
            rhs.name,
            tProj,
            operator,
            true
          );

          // insert hash joined records into temp table
          for (const joinedRec of joinedIterator) {
            if (
              src1 === src2 &&
              joinedRec[`_id${src1}`] !== joinedRec[`_id${src2}`]
            )
              continue; // skip over records from same table with different ids
            DB.insertRecords(tTableName, [joinedRec]);
          }

          // We might need these hashes down below.
          // Potential algoritm improvement: do this only when it guarantees more efficiency
          DB.hashTable(tTableName, `_id`, true);
        }

        const t1 = pairOps[0].lhs.parentTable;
        const t2 = pairOps[0].rhs.parentTable;

        DB.addTable(compositeId, [
          ...groupedCols.get(t1),
          ...groupedCols.get(t2),
        ]);

        if (pairOps.length === 1) {
          // Just copy the first temp table into the composite table for this pair
          for (const rec of DB.getAllRecords(tempNames[0])) {
            DB.insertUniqueRecordsById(compositeId, [rec]);
          }
        } else {
          // Only keep common records
          const firstTempTableName = tempNames[0];
          const remainingTables = tempNames.slice(1);

          for (const rec of DB.getAllRecords(firstTempTableName)) {
            // Test if rec is in all the other temporary tables
            // This is O(n) only since all temp tables already have a hash index on _id
            if (
              remainingTables.every((rtName) =>
                DB.hasValue(rtName, '_id', rec._id)
              )
            ) {
              DB.insertUniqueRecordsById(compositeId, [rec]);
            }
          }
        }

        // hash tables for ids from the two tables in the pair
        // (kept by the hash join algorithm in DB)
        DB.hashTable(compositeId, `_id${t1}`, true);
        DB.hashTable(compositeId, `_id${t2}`, true);

        // printTableEntries(compositeId);

        // drop temp tables
        for (const tName of tempNames) {
          DB.drop(tName);
        }
      }

      /**
       * Map from table name to all composite join tables having the table as a source
       * @type {Map<string, string[]>} */
      const joinMap = new Map();

      tables.forEach((t) => joinMap.set(t, []));
      for (const compositeId of compositeOps.keys()) {
        /** @type {[string, string]} */
        const [t1, t2] = JSON.parse(compositeId);
        joinMap.get(t1).push(compositeId);
        joinMap.get(t2).push(compositeId);
      }

      // Create composite join trees

      /**
       * Join table name tree
       * @typedef {Object} Tree
       * @property {string} id - table id
       * @property {Set<Tree>} children - list of children nodes
       */
      const ids = new Set([...compositeOps.keys()]);

      /**
       * @param {string} id
       * @returns {Tree}
       */
      const constructJoinTableTree = (id) => {
        if (!ids.has(id)) return null;
        ids.delete(id);
        const tree = { id, children: new Set() };

        /** @type {[string, string]} */
        const [t1, t2] = JSON.parse(id);

        joinMap.get(t1).forEach((cId) => {
          const childNode = constructJoinTableTree(cId);
          if (childNode) tree.children.add(childNode);
        });

        joinMap.get(t2).forEach((cId) => {
          const childNode = constructJoinTableTree(cId);
          if (childNode) tree.children.add(childNode);
        });
        return tree;
      };

      /** @type {Tree[]} */
      const trees = [];
      for (const id of ids) {
        const tree = constructJoinTableTree(id);
        if (tree) trees.push(tree);
      }

      // Get intersection and put it into new table

      // It is time to also get independent tables in the select statement
      // to later compute their cross product

      const independentSimpleOps = [...simpleOps.keys()].filter((tName) => {
        for (const cName of compositeOps.keys()) {
          /** @type {[string, string]} */
          const componentTables = JSON.parse(cName);
          if (componentTables.includes(tName)) return false;
        }
        return true;
      });

      const independentTables = [...noOps, ...independentSimpleOps];

      /**
       * Repopulate root table with all the common values in the tables in the the given tree
       * @param {Tree} tree Tree of joined table names
       * @returns {string} Name of generated table with common vals from the given tree
       */
      const getCommonTreeVals = (tree) => {
        // decompose tree
        const { id: parentId, children } = tree;

        //decompose root id
        /** @type {string[]} */
        const parentTables = JSON.parse(parentId);

        if (children.size === 0) {
          // if tree has no children we can use root table as result
          return parentId;
        } else {
          const childrenTables = [...children.values()].map(getCommonTreeVals);
          let results = [];
          for (const parentRec of DB.getAllRecords(parentId)) {
            // Test if rec is in all the other children tables
            if (
              childrenTables.every((cName) => {
                /** @type {string} */
                const childTables = JSON.parse(cName);
                // intersect tables in parent and child, guaranteed size 1
                const intersectionTable = parentTables.filter((parentTable) =>
                  childTables.includes(parentTable)
                )[0];
                DB.hasValue(
                  cName,
                  `_id${intersectionTable}`,
                  parentRec[`_id${intersectionTable}`]
                );
              })
            ) {
              // construct record by hash joining in memory
              // could also use DB.hashJoin with more temp tables but this is much simpler
              const composedRec = Object.assign({}, parentRec);
              let recs = [composedRec];
              childrenTables.forEach((cName) => {
                /** @type {string} */
                const childTables = JSON.parse(cName);
                // intersect tables in parent and child, guaranteed size 1
                const intersectionTable = parentTables.filter((parentTable) =>
                  childTables.includes(parentTable)
                )[0];
                const childrenRecIterator = DB.getRecsFromHash(
                  cName,
                  `_id${intersectionTable}`,
                  '=',
                  parentRec[`_id${intersectionTable}`]
                );
                const temp = [];
                for (const childRec of childrenRecIterator) {
                  for (const r of recs) {
                    const composedRec = Object.assign({}, r, childRec);
                    temp.push(composedRec);
                  }
                }
                recs = temp;
              });
              results.push(
                ...recs.map((rec) => {
                  const keysToKeep = [
                    `_id${parentTables[0]}`,
                    `_id${parentTables[1]}`,
                    ...columns.map((c) => c.name),
                  ];
                  const res = {};
                  for (const key of keysToKeep) {
                    if (key in rec) {
                      res[key] = rec[key];
                    }
                  }
                  return res;
                })
              );
            }
          }
          DB.clearTable(parentId);
          DB.insertRecords(parentId, results);
          parentTables.forEach((t) => DB.hashTable(parentId, `_id${t}`, true));
          return parentId;
        }
      };

      for (const tree of trees) {
        independentTables.push(getCommonTreeVals(tree));
      }

      // We now have a list of independent tables in independentTables
      // that we can perform block join on to get their cross product

      /**
       * Create projection object from col/table pair
       * @param {string} col Column name
       * @param {string} tName Table name
       * @returns Projection object for db
       */
      const mapColToProjection = (col, tName) => ({
        colNameDst: col,
        colSrc: [tName, col],
      });

      /**
       * Performs block join on all the independent tables in sequence
       * @returns {string} Name of temp table results are stored in
       */
      const blockJoinIndependentTables = () => {
        if (independentTables.length === 1) {
          // There is no joining needed, just move into new table and remove duplicates
          const table = independentTables[0];
          const finalTableName = nanoid();
          DB.addTable(finalTableName, columns);
          for (const finalRec of DB.getAllRecords(table)) {
            const res = addRecordAsId(deleteIds(finalRec));
            DB.insertUniqueRecordsById(finalTableName, [res], true);
          }
          return finalTableName;
        }
        // Block join tables in sequence while removing duplicate entries
        let t1 = null,
          t2 = null;
        do {
          if (t1 === null) {
            t1 = independentTables.shift();
          }
          let t2 = independentTables.shift();

          const t1NonIdCols = getNonIdCols(t1);
          const t2NonIdCols = getNonIdCols(t2);
          const allNonIdCols = [...t1NonIdCols, ...t2NonIdCols];

          const joinedIterator = DB.blockJoin(t1, t2, [
            ...t1NonIdCols.map((column) => mapColToProjection(column, t1)),
            ...t2NonIdCols.map((column) => mapColToProjection(column, t2)),
          ]);

          const tTableName = nanoid();
          DB.addTable(tTableName, allNonIdCols);

          for (const joinedRec of joinedIterator) {
            const recToInsert = addRecordAsId(joinedRec);
            DB.insertUniqueRecordsById(tTableName, [recToInsert]);
          }

          DB.drop(t1);
          t1 = tTableName;
        } while (independentTables.length > 0);

        return t1;
      };

      const finalTempTable = blockJoinIndependentTables();

      DB.clearTable(tempTableName);

      for (const finalRec of DB.getAllRecords(finalTempTable)) {
        DB.insertUniqueRecordsById(tempTableName, [finalRec]);
        DB.insertUniqueRecordsById(resultTableName, [finalRec]);
      }

      DB.drop(finalTempTable);
      [...compositeOps.keys()].forEach((compositeId) => DB.drop(compositeId));
    };

    execute(sortOps());
  }

  const finalNumberOfEntries = DB.getNumberOfEntries(resultTableName);
  return finalNumberOfEntries - initialNumberOfEntries;
}

/**
 * Constructs filter function for record-by-record search in db
 * @param {Operation[]} ops List of operations
 * @param {string} tableName Name of table
 * @param {any} [currObj={}] Current record from which to fetch rhs
 * @returns {Function} Resulting filter function
 */
function constructFilter(ops, tableName, currObj = {}) {
  if (!ops) return (record) => true;

  const filters = new Array();

  ops.forEach((op) => {
    const { lhs, operator, rhs } = op;

    const tableCols = DB.getTableKeys(tableName);

    if (
      tableName === lhs.parentTable &&
      tableCols.find((col) => col === lhs.name)
    ) {
      const rhsVal = rhs instanceof Column ? currObj[rhs.name] : rhs;

      switch (operator) {
        case '=':
          // Igoring type equality to match strings with ints if needed
          filters.push((record) => record[lhs.name] == rhsVal);
          break;

        case '>':
          filters.push((record) => record[lhs.name] > rhsVal);
          break;

        default:
          throw new Error('Unsupported operator');
      }
    }
  });
  let filterFunction = (record) => true;
  if (filters.length > 0)
    filterFunction = (record) => filters.every((filter) => filter(record));
  return filterFunction;
}

/**
 *
 * @param {Query} query query to execute
 */
function executeSingleQuery(query) {
  const { withDecl, nonrecTerm, recTerm, resultTableName } = query;
  const recTableName = createRecursiveTable(withDecl);
  DB.addTable(resultTableName, DB.getTableKeys(recTableName));
  executeTerm(nonrecTerm, recTableName, resultTableName);

  let recordsInserted = 0;
  do {
    recordsInserted = executeTerm(recTerm, recTableName, resultTableName);
  } while (recordsInserted > 0);
  DB.drop(recTableName);
}

module.exports = {
  executeSingleQuery,
};
