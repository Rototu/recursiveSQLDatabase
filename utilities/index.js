const chalk = require('chalk');
const now = require('performance-now');

/**
 * Standard zip function for two arrays of equal length
 * @template T1, T2
 * @param {T1[]} arr1 First array
 * @param {T2[]} arr2 Second array
 * @returns {(T1|T2)[][]} Zipped result
 */
function zip2(arr1, arr2) {
  if (arr1.length !== arr2.length)
    throw new Error('Arrays must be of same length to zip them.');

  return arr1.map((arr1El, indexOfEl) => [arr1El, arr2[indexOfEl]]);
}

/**
 * Removes duplicate objects from array (O(n^2))
 * @param {any[]} arr Array of objects with duplicates
 * @returns {any[]} Array of objects without duplicates
 */
function eliminateObjectDuplicates(arr) {
  return [
    ...arr
      .reduce((map, obj) => map.set(JSON.stringify(obj), true), new Map())
      .keys(),
  ].map(JSON.parse);
}

/**
 * Removes a property from all objects in the input array
 * @param {any[]} objects Array of objects
 * @param {string} keyToRemove Property to remove from objects
 * @returns {any[]}
 */
function eliminateObjectKeys(objects, keyToRemove) {
  return objects.map((obj) => {
    // eslint-disable-next-line no-unused-vars
    const { [keyToRemove]: removed, ...rest } = obj;
    return rest;
  });
}

/**
 * Pauses execution for given amount of milliseconds
 * @param {number} milliseconds Time in milliseconds to wait for
 */
function wait(milliseconds) {
  const start = now();
  let end = null;
  do {
    end = now();
  } while (end - start < milliseconds);
}

/**
 * Logs the time needed for execution of code;
 * @param {number} start Start time of code execution
 * @param {number} end End time of code execution
 * @param {string} codeSectionName Name of code section profiled
 */
function logTime(start, end, codeSectionName) {
  const time =
    end - start > 1000
      ? `${((end - start) / 1000).toFixed(2)}s`
      : `${(end - start).toFixed(0)}ms`;
  console.info(
    `Execution time for ${chalk.bgBlackBright(codeSectionName)}: ${chalk.green(
      time
    )}`
  );
}

module.exports = {
  zip2,
  eliminateObjectDuplicates,
  wait,
  logTime,
  eliminateObjectKeys,
};
