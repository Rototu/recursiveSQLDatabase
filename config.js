module.exports = {
  /** Time in ms before returning iterator for page records,
  simulates physical storage access time. */
  pageFetchTime: 0.1,
  /** Maximum amoung of records one page can store */
  maxNoOfRecsPerFile: 100,
  cacheOptions: {
    /** Maximum number of pages the buffer can store */
    max: 50,
  },
  /** Size of block for blockjoin */
  blockJoinSize: 100,
  /** 
   * Tests will repeat algorithm for each scale in array.
   * If running the standard tests, the scales represent 
   * how many records to load from input csv files, in percentages (0-100).
   * If running the graph test, the scales will represent the connectivity of the generated graph.
   * If running the sorting query test, these scales will represent the number of entries generated.
   */
  scales: [10,25,50,100],
  /** How many runs to do for each algorithm (first one will be discarded) */
  noOfRuns: 5,
};
