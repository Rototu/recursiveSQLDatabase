// libraries
const now = require('performance-now');
const chalk = require('chalk');
const { plot } = require('nodeplotlib');
const minimist = require('minimist');

// own code modules
const { parseQueriesFromFile } = require('./query');
const { parseCsvFile } = require('./db/csvInit');
const {
  executeSingleQuery: standarddbExecute,
} = require('./evaluators/standarddb');
const {
  executeSingleQuery: recursivedbExecute,
} = require('./evaluators/recursivedb');
const DB = require('./db'); // imported for type annotation
const {
  pageFetchTime,
  maxNoOfRecsPerFile,
  cacheOptions,
  scales,
  blockJoinSize,
  noOfRuns,
} = require('./config');
const { logTime } = require('./utilities');
const genRandomCsvGraph = require('./graphGen');

const args = minimist(process.argv.slice(2));
const { n } = args;

console.log(
  `Starting custom graph test with the following options (change in ${chalk.blackBright(
    './config.js'
  )}):
Maximum number of records per page: ${chalk.green(maxNoOfRecsPerFile)}
Page buffer capacity: ${chalk.green(cacheOptions.max + ' pages')}
Page fetch time (to simulate physical storage access time): ${chalk.green(
    pageFetchTime + 'ms'
  )}
Block join block size: ${chalk.green(blockJoinSize)}\n`
);

// here the scale represents the connectivity of the graph
let start = now();
for (const i of scales) {
  genRandomCsvGraph(n, i, `g${i}.csv`);
}
let end = now();
logTime(start, end, 'generate csv graphs');

// PARSE SQL QUERY
start = now();
const parsedQueries = parseQueriesFromFile(`./dataset/graphQuery.txt`);
end = now();
logTime(start, end, 'parsing recursive sql query');

console.info(chalk.red('\nStandardDB algorithm test\n'));
const standardtimes = [];
scales.forEach((scale) => {
  console.info(
    `\nDoing graph benchmark with ${scale}% connectivity between nodes\n`
  );
  // LOAD TABLES FROM CSV INTO DB
  start = now();
  parseCsvFile(`./dataset/g${scale}.csv`, 'g', 100);
  end = now();
  logTime(start, end, 'loading data into db from csv files');

  DB.printStats();

  // Execute queries
  let totalTime = 0;
  for (let run = 0; run < noOfRuns; run++) {
    let runStart = now();
    parsedQueries.forEach((query) => {
      start = now();
      standarddbExecute(query);
      end = now();
    });
    let runEnd = now();
    logTime(runStart, runEnd, `standarddb to do run ${run}`);
    if (run > 0) totalTime += runEnd - runStart;
    parsedQueries.forEach((query) => {
      DB.drop(query.resultTableName);
    });
  }
  console.log(
    `\nAverage time for standarddb to do a run is ${chalk.green(
      (totalTime / (1000 * (noOfRuns - 1))).toFixed(3)
    )}s`
  );

  standardtimes.push((totalTime / (1000 * (noOfRuns - 1))).toFixed(3));

  DB.dropAllTables();
});

DB.dropAllTables();

console.info(chalk.red('\nRecursiveDB algorithm test\n'));
const recursiveTimes = [];
scales.forEach((scale) => {
  console.info(
    `\nDoing graph benchmark with ${scale}% connectivity between nodes\n`
  );
  // LOAD TABLES FROM CSV INTO DB
  start = now();
  parseCsvFile(`./dataset/g${scale}.csv`, 'g', 100);
  end = now();
  logTime(start, end, 'loading graph into db from csv file');

  DB.printStats();

  // Execute queries
  let totalTime = 0;
  for (let run = 0; run < noOfRuns; run++) {
    let runStart = now();
    parsedQueries.forEach((query) => {
      start = now();
      recursivedbExecute(query);
      end = now();
    });
    let runEnd = now();
    logTime(runStart, runEnd, `time for recursivedb to do run ${run}`);
    if (run > 0) totalTime += runEnd - runStart;
    parsedQueries.forEach((query) => {
      DB.drop(query.resultTableName);
    });
  }
  console.log(
    `\nAverage time for recursivedb to do a run is ${chalk.green(
      (totalTime / (1000 * (noOfRuns - 1))).toFixed(3)
    )}s`
  );

  recursiveTimes.push((totalTime / (1000 * (noOfRuns - 1))).toFixed(3));

  DB.dropAllTables();
});

const data = [
  { x: scales, y: standardtimes, type: 'line', name: 'StandardDB times' },
  { x: scales, y: recursiveTimes, type: 'line', name: 'RecursiveDB times' },
];

const xAxisTemplate = {
  title: 'Graph connectivity (%)',
};

const yAxisTemplate = {
  title: 'Time taken (s)',
};

const layout = { xaxis: xAxisTemplate, yaxis: yAxisTemplate };

console.info('\nRendering plot at http://localhost:8991/plots/0/index.html');

plot(data, layout);
