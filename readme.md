# An attempt at a recursive database

This project was initially written for my Database Sytems Implementation exam. It's a small database engine supporting recursive queries of the following type:

```SQL
WITH RECURSIVE t(c1, c2) AS (
  SELECT * FROM a
  UNION
  SELECT a.c1, t.c2 FROM a, t WHERE t.c1 = a.c2 AND t.c2 > t.c1 AND a.c2 > a.c1
)
SELECT * INTO n FROM t;
```

I have provided two ways of execution for them (a straightforward algorithm, "StandardDB", and an optimised one, "RecursiveDB"). 

The exact type of syntax that can be parsed can be seen in the `dataset\query1.txt`, `dataset\query2.txt` and `dataset\queryOrdered.txt` files . No other keywords than those is the example files are supported, but tables can have any number of columns and the recursive instruction can have any number of comparison operations. 

To see how to use the the db engine, take a look at `index.js` in the root folder.

## Assumptions

Node.js and NPM are installed. 
Another assumption is that the code will run on a Linux-like console due
to the way environment variables are set through the npm commands in `package.json`.


## Installation steps

Before running the code, download all the dependencies from npm:

```[bash]
npm install
```

## Before runing tests: configuration of the database parameters

The database has configurable properties such as the amount of records 
a page can store or how long it takes to retrieve one. 
You can modify all this in the __config.js__ file in the root folder.

## How to run tests for given benchmarks and queries

To run the code on the datasets and queries given with the exam, run the following command:

```[bash]
npm run benchmark -- --batchNumber 1 --queryNumber 1
```

You can replace the 1s with the batch number and query number 
you desire (batches: 1-3, queries: 1-2). 
The command will run the selected query on the 
selected dataset with both the StandardDB and RecursiveDB algorithms.

## How to run graph benchmark

Run the following command:

```[bash]
npm run graph -- --n 1000
```

You can replace the 1000 with the number of nodes you want 
the generated graphs to have. For details about what 
these implies, see project report.

## How to run ordering benchmark

Run the following command

```[bash]
npm run order
```

Important: the *scales* variable in the __config.js__ file will 
represent the number of entries generated, not a percentage.

## How to view generated plots

After running any of the above commands, when the timing of the operations finishes a
server will be started at port 8991 on localhost, serving the generated time plot.
To view it go to <http://localhost:8991/plots/0/index.html>. 
The link should open automatically in a browser tab once all 
the processing is done, but if it does not just copy-paste it 
into the browser. 