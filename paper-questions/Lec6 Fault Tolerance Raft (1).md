# LEC 6: Fault Tolerance: Raft (1)

# Read

[In Search of an Understandable Consensus Algorithm (Extended Version)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)



## Question

> Suppose we have the scenario shown in the Raft paper's Figure 7: a cluster of seven servers, with the log contents shown. The first server crashes (the one at the top of the figure), and cannot be contacted. A leader election ensues. For each of the servers marked (a), (d), and (f), could that server be elected? If yes, which servers would vote for it? If no, what specific Raft mechanism(s) would prevent it from being elected?

![Figure 7](https://github.com/ricemaster/6.824-mit-2020/blob/master/paper-questions/Figure7.png)



## Answer
