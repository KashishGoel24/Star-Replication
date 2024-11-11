# Star replication (STAQ)

CRAQ setup: Head -> S1 -> S2 -> Tail

The issue with CRAQ is that writes flow in only a single direction: from Head to
Tail. The write throughput is therefore limited by the lowest network bandwidth
in this chain.  

Data centers typically offer *full-bisection bandwidth* i.e, no matter how we
cut the servers into two equal halves, each server in first half should be able
to talk to another server in the second half with full bandwidth.  The available
bandwidth between Head -> Tail, Head -> S2, S1 -> Tail is not properly utilized
by CRAQ.

Further, networks nowadays are *full-duplex* i.e, in a C1 - C2 link, both C1
and C2 can send data to each other with full bandwidth. Bandwidth in the
opposite direction (Tail -> Head, S2 -> S1, etc) is also underutilized by CRAQ.
It only passes write acknowledgements, but never the actual heavy-weight data in
the opposite direction.

Problems happen in a geo-distributed CRAQ setup as well. Having an
enforced order severely limits flexibility/adaptability. Let us say our chain is
India -> UK -> US. If India -> UK link is unhealthy, we cannot start sending
data India -> US -> UK or US -> UK -> India.

Under star replication, *any server* can receive the writes. This server can
forward the writes via *any arbitrary chain* that covers all the servers.
However, we need to be careful and maintain linearizability.

CRAQ maintains linearizability using three ideas: 
1. Head is the only one that can assign new version numbers. Due to this, version
numbers are always in strictly increasing order. 
2. A write is acknowledged only after *every replica* has seen the write.  When
there is a read during inflight writes (dirty versions), replicas ping the tail
and return the version known to the tail. Otherwise, they are safe to respond to
reads directly.
3. Tail applies the write only after every server has seen the update.

We observe that we *do not* need writes to follow the head->tail chain order as
long as we can maintain the above guarantees! (We have not thought about
failovers. We will think about them later.) 

We show that writes can go to *any* server. This server can send the write via
*any* path to everyone else. Similar to CRAQ, we assume that each write
goes via a path and then the acknowledgements come in the opposite direction on
the same path.

There are three main changes we make:
1. Since writes can be accepted by any server, version numbers can not be
assigned immediately. We leave the version number unassigned; head assigns the
version number whenever the request hits the head. Every server is guaranteed to
learn the version number: servers *after* the head on the path will learn it
immediately and servers *before* the head will learn it during acknowledgements.

2. Tail cannot apply a write *until* it is sure that everyone knows that there
is an in-flight write. Otherwise, replicas might incorrectly locally serve
reads. Therefore, we must apply the write when acknowledgement hits the tail. At
this point, tail is sure that everyone is aware that there is an in-flight
write.

3. If write acknowledgements came from the tail, only then can a write be marked
clean. If the server was *before* the tail in the return path, it does not know
when tail has applied the write. Its version will remain dirty. Dirty versions
will become clean asynchronously, either at the time of reads, or during
anti-entropy. Dirty versions may directly become garbage collected if they see
acknowledgements from tail for newer versions.

We can see that we actually do not need separate head and tail. We can just have
one special server that assigns versions on forward request path (writes) and
commits writes on backward request path (acknowledgements).