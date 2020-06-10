# LEC 6: Fault Tolerance: Raft (1)

# Paper

[In Search of an Understandable Consensus Algorithm (Extended Version)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)



## Question

> Suppose we have the scenario shown in the Raft paper's Figure 7: a cluster of seven servers, with the log contents shown. The first server crashes (the one at the top of the figure), and cannot be contacted. A leader election ensues. For each of the servers marked (a), (d), and (f), could that server be elected? If yes, which servers would vote for it? If no, what specific Raft mechanism(s) would prevent it from being elected?

![Figure 7](https://github.com/ricemaster/6.824-mit-2020/blob/master/paper-questions/Figure7.png)



## Answer

In order to get votes, the candidate need to meet two conditions:

- *[Condition 1]* candidate's term should NOT less than voter's

- *[Condition 2]* the voter's `votedFor` is **null** or `candidateId`, and candidate’s log is at least as `up-to-date` as receiver’s log

  - > Raft determines which of two logs is more `up-to-date` by comparing the index and term of the last entries in the logs. 
    >
    > If the logs have last entries with different terms, then the log with the later term is more up-to-date. 
    >
    > If the logs end with the same term, then whichever log is longer is more up-to-date.

    

When (a) starts a election,

- (a)'s current term is **6**
  - (b)'s current term is *4*
  - (c)'s current term is *6*
  - (d)'s current term is *7*
  - (e)'s current term is *4*
  - (f)'s current term is *3*
  - So, (b), (c), (e) and (f) meet the *[Condition 1]*
- (a)'s last entry is **9**, term is **6**
  - (b)'s last entry is *4*, term is *4*
  - (c)'s last entry is *11*, term is *6*
  - (e)'s last entry is *7*, term is *4*
  - (f)'s last entry is *11*, term is *3*
  - (b), (e), (f) meet the *[Condition 2]*

At last, (b), (e), (f) will vote (a), less than majority, then (a) couldn't win the election.



When (d) starts a election,

- (d)'s current term is **7**
  - (a)'s current term is *6*
  - (b)'s current term is *4*
  - (c)'s current term is *6*
  - (e)'s current term is *4*
  - (f)'s current term is *3*
  - So, all peers meet the *[Condition 1]*
- (d)'s last entry is **12**, term is **7**
  - (a)'s last entry is *9*, term is *6*
  - (b)'s last entry is *4*, term is *4*
  - (c)'s last entry is *11*, term is *6*
  - (e)'s last entry is *7*, term is *4*
  - (f)'s last entry is *11*, term is *3*
  - Still, all meet the *[Condition 2]*

At last, all peers will vote (d), and (d) will win the election.



When (f) starts a election,

- (f)'s current term is 3
  - (a)'s current term is *6*
  - (b)'s current term is *4*
  - (c)'s current term is *6*
  - (d)'s current term is *7*
  - (e)'s current term is *4*
  - So, no one meet the *[Condition 1]*

So no one will vote (f), and (f) couldn't win the election.