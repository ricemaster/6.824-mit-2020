# Lab 1: MapReduce

## Roles
![roles](https://github.com/ricemaster/6.824-mit-2020/blob/master/src/mr/img/mr.png)

- Master: Task Assignment, Completion Check
  - as a RPC server
  - re-assign tasks as worker crashed
- Worker: Request Tasks from Master, call map/reduce function(mapF/reduceF)
  - Map: read input, call mapF, create intermediate K/V (as file mr-X-Y)
  - Reduce: read intermediate K/V, call reduceF, write output files
- mapF/reduceF: user-defined functions, do MapReduce Job
  
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