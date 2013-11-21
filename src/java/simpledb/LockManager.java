package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;

public class LockManager {
    private Map<PageId, ArrayList<TransactionId>> reads;
    private Map<PageId, TransactionId> writes;
    private Map<PageId, ReentrantReadWriteLock> locks;
    private Map<TransactionId, ArrayList<TransactionId>> depGraph;

    public LockManager() {
        this.reads = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
        this.writes = new ConcurrentHashMap<PageId, TransactionId>();
        this.locks = new ConcurrentHashMap<PageId, ReentrantReadWriteLock>();
        this.depGraph = new ConcurrentHashMap<TransactionId, ArrayList<TransactionId>>();
    }

    private void addDep(TransactionId tid, PageId pid) {
        if (!depGraph.containsKey(tid)) {
            depGraph.put(tid, new ArrayList<TransactionId>());
        }

        if (reads.containsKey(pid)) {
            for(TransactionId dep : reads.get(pid)) {
                if (!reads.get(pid).contains(tid))
                    if (!depGraph.get(tid).contains(dep)){
                        depGraph.get(tid).add(dep);
                    }
            }
        }

        if (writes.containsKey(pid)) {
            if (tid != writes.get(pid))
                if (!depGraph.get(tid).contains(writes.get(pid))) {
                    depGraph.get(tid).add(writes.get(pid));
                }
        }
    }

    private void removeDep(TransactionId tid) {
        depGraph.remove(tid);
    }

    private synchronized boolean hasDeadlock(TransactionId start) throws TransactionAbortedException {
        ArrayList<TransactionId> q = new ArrayList<TransactionId>();
        q.addAll(depGraph.get(start));
        ArrayList<TransactionId> visited = new ArrayList<TransactionId>();

        while(q.size() > 0) {
            TransactionId tid = q.remove(0);
            for(TransactionId child : depGraph.get(tid)) {
                assert child != null;
                if (visited.contains(child))
                    throw new TransactionAbortedException();
                visited.add(child);
                q.add(0, child);
            }
        }

        return true;
    }

    private Lock readLock(PageId pid) throws TransactionAbortedException {
        if (!locks.containsKey(pid)) {
            locks.put(pid, new ReentrantReadWriteLock());
        }

        Lock lock = locks.get(pid).readLock();
        return lock;
    }

    private Lock writeLock(PageId pid) throws TransactionAbortedException {
        if (!locks.containsKey(pid)) {
            locks.put(pid, new ReentrantReadWriteLock());
        }

        Lock lock = locks.get(pid).writeLock();
        return lock;
    }

    public void readLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (writes.containsKey(pid) && writes.get(pid) == tid) {
            return;
        }

        if (!reads.containsKey(pid)) {
            reads.put(pid, new ArrayList<TransactionId>());
        }

        addDep(tid, pid);
        reads.get(pid).add(tid);
        hasDeadlock(tid);
        readLock(pid).lock();
    }

    public void readUnlock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (!reads.containsKey(pid))
            return;
        if (!reads.get(pid).contains(pid))
            return;
        reads.get(pid).remove(tid);
        removeDep(tid);
        readLock(pid).unlock();
    }

    public void writeLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (reads.containsKey(pid) && reads.get(pid).contains(tid) && reads.get(pid).size() == 1) {
            return;
        }
        addDep(tid, pid);
        writes.put(pid, tid);
        hasDeadlock(tid);
        writeLock(pid).lock();
    }

    public void writeUnlock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        if (!writes.containsKey(pid))
            return;
        if (writes.get(pid) != tid)
            return;
        writes.remove(pid);
        removeDep(tid);
        writeLock(pid).unlock();
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return reads.get(pid).contains(tid) || writes.containsValue(tid);
    }

    public void releasePage(TransactionId tid, PageId pid) {
        try {
            readUnlock(tid, pid);
            writeUnlock(tid, pid);
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    public void transactionComplete(TransactionId tid, boolean commit) {
        // clear write locks
        for(PageId pid : writes.keySet())
            if (writes.get(pid) == tid) {
                try {
                    writeUnlock(tid, pid);
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                }
            }

        // clear read locks
        for(PageId pid : reads.keySet())
            if (reads.get(pid).contains(tid))
                try {
                    readUnlock(tid, pid);
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                }
    }

    public void printGraph() {
        System.out.println("Graph ---");
        for(TransactionId id : depGraph.keySet()) {
            System.out.print(id.getId() + ": ");
            for (TransactionId child: depGraph.get(id))
                System.out.print(" " + child.getId());
            System.out.println();
        }
    }
}
