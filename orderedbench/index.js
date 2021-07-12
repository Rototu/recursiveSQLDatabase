const fs = require('fs');
const { zip2 } = require('../utilities');

/**
 * Generates a permutation of (1,2,...,n) and writes it to a csv file
 * @param {number} n Number of records to generate
 * @param {string} fileName
 */
function generatePermutationCSV(n, fileName) {
  const permutationOfN1 = new Array(n)
    .fill(0)
    .map((_, index) => index + 1)
    .sort(() => Math.random() - 0.5);

  const permutationOfN2 = new Array(n)
    .fill(0)
    .map((_, index) => index + 1)
    .sort(() => Math.random() - 0.5);

  const data = zip2(permutationOfN1, permutationOfN2).map(
    ([n1, n2]) => `${n1},${n2}`
  );

  fs.writeFileSync(`./dataset/${fileName}`, data.join('\n'));
}

module.exports = generatePermutationCSV;
