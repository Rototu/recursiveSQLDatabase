const { zip2 } = require('../../utilities');

/*
Most of the lexing is done in the parsing module actually, 
here we only separate queries if there are multiple of them
*/

/**
 * Separate large text of queries spred over multiple lines into separate one-line queries
 * @param {string} txt Text read from file (sequence of queries)
 * @return {string[]} Array of one-line query strings
 */
function separateQueries(txt) {
  // separate txt into lines and delete empty ones
  let arrayOfLines = txt.match(/[^\r\n]+/g).map((line) => line.trim());

  // compute line where each query starts
  const queryIndeces = [];
  const isQueryStart = (line) => line.indexOf('WITH') == 0;
  arrayOfLines.forEach((line, index) => {
    if (isQueryStart(line)) {
      queryIndeces.push(index);
    }
  });

  // compute pairs of start end end line indeces for each query
  const queryEnds = queryIndeces.slice(1);
  queryEnds.push(arrayOfLines.length);
  const queryDelimitors = zip2(queryIndeces, queryEnds);

  // separate queries
  const separatedQueries = queryDelimitors.map(([startPos, endPos]) =>
    arrayOfLines.slice(startPos, endPos).join(' ')
  );
  return separatedQueries;
}

module.exports = {
  separateQueries,
};
