// libraries
const fs = require('fs');

// own code
const { separateQueries } = require('./lexer');
const { parseQuery } = require('./parser');

/**
 * Parse queries from file
 * @param {string} filepath
 */
function parseQueriesFromFile(filepath) {
  const fileData = fs.readFileSync(filepath, 'utf8');
  const parsedQueries = separateQueries(fileData).map(parseQuery);
  return parsedQueries;
}

module.exports = {
  parseQueriesFromFile,
};
