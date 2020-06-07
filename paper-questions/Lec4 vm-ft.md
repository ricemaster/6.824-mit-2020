# Lec4: Primary-Backup Replication

# Paper

[Fault-Tolerant Virtual Machines (2010)](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)



## Question

> How does VM FT handle network partitions? That is, is it possible that if the primary and the backup end up in different network partitions that the backup will become a primary too and the system will run with two primaries?



## Answer

No, the `split brain` disaster will not happen.

The mechanism to prevent `split brain` is called `TEST-AND-SET`.

VMWare uses a outside authority, a third-party server, to communicate with both primary and backup.  This external tiebreaker tests primary and backup to find out if they're alive, and sets a flag locally to make sure there will be only one primary go live.

