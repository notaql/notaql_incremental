# NotaQL Platform for Incremental Data Transformations

This platform is based on the [NotaQL Platform for Cross-System Transformations](https://github.com/notaql/notaql) and adds the following:

### Incremental Computations 
When a transformation job is executed again, results from the former computation can be reused; only these results plus the changes (the delta) in the base data are needed to compute the new result; a complete recomputation is avoided).

### Different Change--Data-Capture Approaches to detect changes in the input data of a transformation
* Snapshot-based 
  * Create complete database snapshot
  * when transformation starts again, this snapshot is compared with the current database state to compute the delta
* Timestamp-based
  * Timestamps in the input data are used to find inserted and updated items
* Log-based
  * A log file is used to find inserted, updated and deleted items
* Trigger-based
  * A trigger in the input database immediately pushes the changes to the NotaQL platform
  
### Web-based Dashboard
![Dashboard](https://raw.githubusercontent.com/notaql/notaql_incremental/master/img/dashboard.png "Dashboard")

### Advisor
The learning advisor component choses the best possible way to execute a transformation, either full, or timestamp-based, or in some other way.

# About
This project is a research prototype created at the
[Heterogeneous Information Systems Group](http://wwwlgis.informatik.uni-kl.de/cms/his/) at the Technical University of Kaiserslautern.
Contact person is [jschildgen](https://github.com/jschildgen).
We open sourced it in order to allow new contributors to add new features, bug fixes, or simply
use it for their own purposes.
