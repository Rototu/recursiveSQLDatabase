const fs = require('fs');

/**
 * Generates a random graph with custom number of nodes and connectivity. *
 * @param {number} nodeCount number of nodes in graph
 * @param {number} edgePercentageToKeep each node will be connected to nodeCount * edgePercentageToKeep / 100 nodes
 * @param {string} fileName name of file
 */
function genRandomCsvGraph(nodeCount, edgePercentageToKeep, fileName) {
  // generate nodeCount nodes v1, v2, ...
  const nodes = new Array(nodeCount).fill(0).map((_, index) => `v${index}`);
  let edges = [];

  // generate random edges
  for (const [index, v1] of nodes.entries()) {
    // random permutation of the list of nodes
    const nodesPermutation = nodes
      .slice(index + 1)
      .sort(() => Math.random() - 0.5);

    // keep only a percentage of the nodes
    const nodesToKeep = nodesPermutation.slice(
      0,
      ((edgePercentageToKeep * nodeCount) / 100).toFixed(0)
    );

    // add edges to graph
    for (const v2 of nodesToKeep) {
      edges.push({ v: v1, w: v2 });
      edges.push({ v: v2, w: v1 });
    }
  }

  edges = edges.map((edge) => `${edge.v},${edge.w}`);

  fs.writeFileSync(`./dataset/${fileName}`, edges.join('\n'));
}

module.exports = genRandomCsvGraph;
