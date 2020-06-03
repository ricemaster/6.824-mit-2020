# Something about Lab 1: MapReduce

## Roles: Master, Worker, user-defined function, and files
![roles](https://github.com/ricemaster/6.824-mit-2020/blob/master/src/mr/img/mr.png)

- Master: Task Assignment, Completion Check
  - as a RPC server
  - re-assign tasks as worker crashed
- Worker: Request Tasks from Master, call map/reduce function(mapF/reduceF)
  - Map: read input, call mapF, create intermediate K/V (as file mr-X-Y)
  - Reduce: read intermediate K/V, call reduceF, write output files
- mapF/reduceF: user-defined functions, do MapReduce Job
- files
  - input files: pg-*.txt
  - intermedia files: mr-X-Y, which X means MapTaskNo. and Y means ReduceTaskNo.
    - create using temp file name, and rename it after finish reducing
  - output files: mr-out-Y
  
## Sequence
![Sequence](https://github.com/ricemaster/6.824-mit-2020/blob/master/src/mr/img/seq.png)
- Master
  - init: list all input file names; phase=map
  - start server
  - assign map/reduce task if not get "finish" response from worker in 10 seconds
  - re-assign task if not 
  - check all task completion using channel
- Worker
  - request task
  - map or reduce
  - exit while getting a "all task done" signal