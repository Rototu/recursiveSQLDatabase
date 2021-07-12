const { Column, Operation, Term, WithDecl, Query } = require('../classes');

/**
 * Parses the table and column names in the WITH part of the recursive query
 * @param {string} string One-line query string
 * @returns {WithDecl} Parsed table name and Column class instances
 */
function parseWithDecl(string) {
  const [table, ...colStrings] = string
    .match(/WITH RECURSIVE (.*) AS/)[1]
    .split(/\(|\)|,/)
    .slice(0, -1);
  const cols = colStrings.map((colName) => new Column(`${table}.${colName}`));
  return new WithDecl(table, cols);
}

/**
 * Parses a string containing a list of columns in a term
 * @param {string} colListStr List of columns (tName.cName) separated by commas
 * @returns {Column[]} List of constucted Column class instances
 */
function parseCols(colListStr) {
  const cols = colListStr.split(/,\s?/).map((colStr) => new Column(colStr));
  return cols;
}

/**
 * Parses a string listing table names separated by commas
 * @param {string} tableListStr String listing table names separated by commas
 * @returns {string[]} Parsed array of table names
 */
function parseTables(tableListStr) {
  const tables = tableListStr.split(/,\s?/);
  return tables;
}

/**
 * Parses a string listing operations separated by commas
 * @param {string} opListStr String listing operations separated by commas
 * @returns {Operation[]} Array of parsed Operation class instances
 */
function parseOps(opListStr) {
  const ops = opListStr.split(/ AND /).map((opStr) => new Operation(opStr));
  return ops;
}

/**
 * Parses a recursive or non-recursive term in the query
 * @param {string} term String for recursive or nonrecursive term in query
 * @returns {Term}
 */
function parseTerm(term) {
  const simpleTermRegex = /SELECT (.*) FROM (.*)/;
  const whereTermRegex = /SELECT (.*) FROM (.*)(?: WHERE (.+))/;
  const hasWhere = (str) => str.match(/WHERE/) != null;
  let cols,
    tables,
    ops = null;
  if (hasWhere(term)) {
    [cols, tables, ops] = term.match(whereTermRegex).slice(1, 4);
  } else {
    [cols, tables] = term.match(simpleTermRegex).slice(1, 3);
  }
  return new Term(
    parseCols(cols),
    parseTables(tables),
    ops ? parseOps(ops) : null
  );
}

/**
 * Parses the terms in the query
 * @param {string} string One-line query string
 * @returns {{nonrecTerm: Term, recTerm: Term}}
 */
function parseTerms(string) {
  const [nonrec, rec] = string.match(/\( (.*) UNION (.*) \)/).slice(1, 3);
  return {
    nonrecTerm: parseTerm(nonrec),
    recTerm: parseTerm(rec),
  };
}

/**
 * Return the name of the destination table for the results
 * @param {string} string One-line query string
 * @returns {string}
 */
function parseDestination(string) {
  const resultTableName = string.match(/SELECT \* INTO (.*) FROM/)[1];
  return resultTableName;
}

/**
 * Parses query from string
 * @param {string} queryStr One-line query string
 * @returns {Query}
 */
function parseQuery(queryStr) {
  const withDecl = parseWithDecl(queryStr);
  const { nonrecTerm, recTerm } = parseTerms(queryStr);
  const destinationTableName = parseDestination(queryStr);
  return new Query(withDecl, nonrecTerm, recTerm, destinationTableName);
}

module.exports = {
  parseQuery,
};
