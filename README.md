# Implementation of an ETL process

This is a course project whose purpose is to optimize by any means a specific processing of some data (see SQL statement below).

I wrote a Java multi-threaded implementation of the query that runs about 35-50 times faster than the SQL statement with PostgreSQL depending on the machine (tested on a 4-core computer and on a 32-core server, see Performance results section below). I did not use any Java data structure for the core processing given their poor performance so that the code would be easily transposable to C code for better performances (about 1.5 time as faster).

## SQL query
The query to be optimized creates a hypercube from three tables:

* Clients
```
 Column |  Type   |       Modifiers
--------+---------+------------------------
 id     | integer | not null
 type   | integer | not null
 geo    | integer | not null
 misc   | integer | not null
```
* Contracts
```
 Column |  Type   |        Modifiers
--------+---------+-------------------------
 id     | integer | not null
 client | integer | not null
 nature | integer | not null
 start  | integer | not null default 201410
 end    | integer |
```
* Invoices
```
 Column      |     Type      |        Modifiers
-------------+---------------+-------------------------
 id          | integer       | not null
 contract    | integer       |
 time        | integer       |
 amount      | numeric(10,2) | not null
 consumption | numeric(9,0)  | not null
```

Here is the SQL statement that defines the hypercube:
```SQL
CREATE TABLE hypercube AS
SELECT
    -- Dimensions
    geo, type, misc, nature, time,
    -- Measures
    SUM(consumption) AS consumption,
    SUM(amount) AS amount,
    COUNT(DISTINCT c.id) AS nclients,
    COUNT(DISTINCT k.id) AS ncontracts,
    COUNT(*) AS ninvoices
FROM clients AS c
JOIN contracts AS k ON c.id = k.client
JOIN invoices AS i ON i.contrat = k.id
GROUP BY type, geo, misc, nature, time;
```

## Running the Java file
Once the Java file is compiled, run it as follows:
```
java ETL data_folder output_file [-n nb_threads] [-p nb_thread_pools] [-s chunk_size] [-l log_type]
```

where:

* *data_folder* must contain the input files as follows: *clients.csv*, *contracts.csv*, as comma-separated CSV files with a header line, and *invoices.bin*, as a big-endian binary file (int, int, byte, float, short, padding byte).
* *output_file* is the file in which the hypercube is written as a comma-separated CSV file.
* *nb_threads* is the number of threads to be used (by default, this is the number of processors available to the Java virtual machine).
* *nb_thread_pools* is the number of thread pools to be used (by default, this equals the number of threads).
* *chunk_size* is the size of the chunks of the invoice file that are sequentially read by the threads while processing the invoices (by default, 16384). It must be a multiple of 16 (size of an invoice).
* *log_type* defines the type of ouput log: 0 for detailed log (default), 1 for compact log, 2 for no log.

## Performance results
Machine specs: Intel Core i7-3610QM @ 2.30GHz (4 cores, 8 logical cores), 8GB RAM DDR3 @ 1600Mhz, SSD storage.

Input data: 1M clients, 1.6M contracts, 57.6M invoices.

Here are the execution times:

* PostgreSQL: 422s
* Optimized implementation (8 threads): 11.5s (including writing the output file), i.e. 37 times faster

PostgreSQL does not make the most of multi-core machines as it processes the data with only one thread.

Here is the evolution of the processing rate depending on the number of threads. As expected, we can see a stabilization above 8 threads (on an octo-core machine).

![Alt text](/relative/path/to/img.jpg?raw=true "Processing rate depending on the number of threads")