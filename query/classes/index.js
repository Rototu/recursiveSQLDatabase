/**
 * Query class
 */
class Query {
  /**
   * Creates a Parsed Query
   * @param {WithDecl} withDecl Parsed WITH section of query
   * @param {Term} nonrecTerm Parsed non-recursive term of query
   * @param {Term} recTerm Parsed recursive term of query
   * @param {string} resultTableName Name of table to store results in
   */
  constructor(withDecl, nonrecTerm, recTerm, resultTableName) {
    this._withDecl = withDecl;
    this._nonrecTerm = nonrecTerm;
    this._recTerm = recTerm;
    this._resultTableName = resultTableName;
  }

  /**
   * Get WITH section of query
   * @returns {WithDecl}
   */
  get withDecl() {
    return this._withDecl;
  }

  /**
   * Get non-recursive term of query
   * @returns {Term}
   */
  get nonrecTerm() {
    return this._nonrecTerm;
  }

  /**
   * Get recursive term of query
   * @returns {Term}
   */
  get recTerm() {
    return this._recTerm;
  }

  /**
   * Get name of table to store results in
   * @returns {string}
   */
  get resultTableName() {
    return this._resultTableName;
  }
}

/**
 * Column class
 */
class Column {
  /**
   * Construct a Column class instance
   * @param {string} colString String of format 'tableName.colName'
   */
  constructor(colString) {
    if (colString === '*') {
      this._colName = null;
      this._tableName = null;
      this._allCols = true;
    } else {
      const [tableName, colName] = colString.split('.');
      this._tableName = tableName;
      this._colName = colName;
      this._allCols = false;
    }
  }

  /**
   * Test if 'SELECT * FROM ...' was used
   * @returns {boolean}
   */
  shouldFetchAllColumns() {
    return this._allCols;
  }

  /**
   * @returns {string} Name of column
   */
  get name() {
    return this._colName;
  }

  /**
   * @returns {string} Name of parent table
   */
  get parentTable() {
    return this._tableName;
  }

  /**
   * Equality test for two columns
   * @param {Column} col Column to compare with
   * @returns {boolean}
   */
  equals(col) {
    return this._colName === col.name && this._tableName === col.parentTable;
  }
}

/**
 * Operation class
 */
class Operation {
  /**
   * Constructs an Operation from an operation string of form tName.colName (> | =) tname.colName
   * @param {string} opStr Operation string
   */
  constructor(opStr) {
    const [lhsOperand, operator, rhsOperand] = opStr.split(' ');
    if (!['=', '>'].includes(operator)) {
      throw new Error(`Unsupported operator ${operator}`);
    }
    this._lhs = new Column(lhsOperand);
    if (rhsOperand.includes('.')) {
      this._rhs = new Column(rhsOperand);
    } else if (isNaN(rhsOperand)) {
      this._rhs = rhsOperand;
    } else {
      this._rhs = parseInt(rhsOperand);
    }
    this._operator = operator;
  }

  /**
   * Test if both sides of operation are Column class instances
   * @returns {boolean}
   */
  hasBothSidesCols() {
    const { _rhs, _lhs } = this;
    return _rhs instanceof Column && _lhs instanceof Column;
  }

  /**
   * Swaps lhs and rhs of the operation if they are both columns
   */
  swapCols() {
    if (this.hasBothSidesCols())
      [this._lhs, this._rhs] = [this._rhs, this._lhs];
  }

  /**
   * @returns {Column}
   */
  get lhs() {
    return this._lhs;
  }

  /**
   * @returns {Column | number | string}
   */
  get rhs() {
    return this._rhs;
  }

  /**
   * @returns {string}
   */
  get operator() {
    return this._operator;
  }
}

/**
 * Query term class
 */
class Term {
  /**
   * Constructs a Query Term
   * @param {Column[]} cols List of Columns
   * @param {string[]} tables List of table names
   * @param {Operation[]?} ops List of Operations
   */
  constructor(cols, tables, ops) {
    this._cols = cols;
    this._tables = tables;
    this._ops = ops;
  }

  /**
   * Get cols
   * @returns {Column[]}
   */
  get cols() {
    return this._cols;
  }

  /**
   * @returns {string[]}
   */
  get tables() {
    return this._tables;
  }

  /**
   * @returns {Operation[]?}
   */
  get ops() {
    return this._ops;
  }

  /**
   * Test if term has any WHERE clause
   * @returns {boolean}
   */
  hasOperations() {
    return this._ops != null;
  }
}

/**
 * Class containing info about the declaration of the recursive table (name and columns)
 */
class WithDecl {
  /**
   * Constructs the WITH Declaration section of the query
   * @param {string} tableName Name of recursive table
   * @param {Column[]} columnList Array of columns
   */
  constructor(tableName, columnList) {
    this._tableName = tableName;
    this._columnList = columnList;
  }

  /**
   * @return {string} Name of result table
   */
  get name() {
    return this._tableName;
  }

  /**
   * @return {string[]} Names of result table columns
   */
  get columns() {
    return this._columnList.map((column) => column.name);
  }
}

module.exports = {
  Query,
  Column,
  Operation,
  Term,
  WithDecl,
};
