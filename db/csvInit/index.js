const fs = require('fs');
const { zip2 } = require('../../utilities');
const DB = require('../');

/**
 * Separates output of fs.readFile into lines
 * @param {string} readText Output of fs.readFile
 * @returns {string[]}
 */
function getFileLines(readText) {
  // separate txt into lines and delete empty ones
  const arrayOfLines = readText.match(/[^\r\n]+/g).map((line) => line.trim());
  return arrayOfLines;
}

/**
 * Parse given csv file and create db table from it
 * @param {string} filePath Path to csv file
 * @param {string} tableName Name of table to create
 * @param {number} scale Percentage of records in file to keep
 * @param {string[] | null} [colNames] Name of columns in csv
 */
function parseCsvFile(filePath, tableName, scale, colNames = null) {
  const rawDbData = fs.readFileSync(filePath, 'utf8');
  const fileLines = getFileLines(rawDbData);

  /**
   * @type {string[]}
   */
  let fieldNames;
  if (!colNames) {
    // create automatic column names: c1, c2, ...
    const noOfCols = fileLines[0].split(',').length;
    fieldNames = new Array(noOfCols)
      .fill(null)
      .map((_, index) => `c${index + 1}`);
  } else {
    fieldNames = colNames;
  }

  DB.addTable(tableName, fieldNames);

  // zip on column names and csv values
  // to then create object using each tuple as key-value pair
  const docs = fileLines.map((csvLine) => {
    const keyValPairs = zip2(fieldNames, csvLine.split(','));
    const res = Object.fromEntries(keyValPairs);
    res._id = JSON.stringify(res); // using record as id to remove duplicates
    return res;
  });

  DB.insertUniqueRecordsById(
    tableName,
    docs.slice(0, Math.round((docs.length * scale) / 100))
  );
}

module.exports = {
  parseCsvFile,
};
