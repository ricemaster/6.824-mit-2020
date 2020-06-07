# Lec4: Primary-Backup Replication

# Paper

[Fault-Tolerant Virtual Machines (2010)](https://pdos.csail.mit.edu/6.824/papers/vm-ft.pdf)



## Question

> How does VM FT handle network partitions? That is, is it possible that if the primary and the backup end up in different network partitions that the backup will become a primary too and the system will run with two primaries?



## Answer

No, the `split brain` disaster will not happen.

The mechanism to prevent `split brain` is called `TEST-AND-SET`.

The system uses shared disk as storage for both primary and backup. There's a `TEST-AND-SET` service running on this shared disk server. Like an external tiebreaker, this server responses the test from primary and backup, and sets a flag locally to make sure there will be only one replica go live to be the primary. If the primary and backup lose network contact with each other, they will both talk to `TEST-AND-SET` service trying to be the primary of the system, and usually the service allows the server who called first.