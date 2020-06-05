# Lec3: GFS 

# Paper

[The Google File System (2003)](https://pdos.csail.mit.edu/6.824/papers/gfs.pdf)



## Question

> Describe a sequence of events that would result in a client reading stale data from the Google File System.



## Answer

1. `Client-1 (C1)` requests to ***read*** `X`  -----> `Master (M)`
2. `C1` got a list of chunkservers and picks the nearest chunkserver who is `Secondary-1(S1)` of this replication group
3. `C1` cached `S1` locally and ***reads*** `X`
4. `Client-2 (C2)` requests to ***write*** `X` -----> `M`
5. The network between `S1` and `M` become **disconnected**
6. `M` ***re-replicates*** a new chunk for `X` as `Secondary-3`, because  the number of available replicas falls below a user-speciﬁed goal
7. `C2` get a list of chunkservers including `Primary-1 (P1)`, `Secondary-2 (S2)`, `Secondary-3 (S3)`
8. `C2` ***write*** X to `P1/S2/S3`
9. `C1` try to ***read*** `X` again from cached `S1` and get the stale data