package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.Timestamp;
import java.util.Date;
import java.io.*;

public class LockManager {
    private static final int TIMEOUT = 10;
    private static final int MAX_COUNT = 10;
    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> reads;
    private ConcurrentHashMap<TransactionId, ArrayList<PageId>> locks;
    private ConcurrentHashMap<PageId, TransactionId> writes;
    private ConcurrentHashMap<TransactionId, Integer> xactsCount;

    public LockManager() {
        writes = new ConcurrentHashMap<PageId, TransactionId>();
        reads = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
	    locks = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
	    xactsCount = new ConcurrentHashMap<TransactionId, Integer>();
    }

    private ArrayList<PageId> getPages(TransactionId tid, PageId pid) {
        ArrayList<PageId> pages;
        if (locks.containsKey(tid)) 
	        pages = locks.get(tid);
        else {
            pages = new ArrayList<PageId>();
            locks.put(tid, pages);
        }
        return pages;
    }

    private boolean hasExclusiveLock(TransactionId tid, PageId pid) {
	    return writes.get(pid) == tid;
    }

    private ArrayList<TransactionId> getReads(PageId pid) {
        ArrayList<TransactionId> xacts = reads.get(pid);
        if (xacts == null)
            reads.put(pid, new ArrayList<TransactionId>());
        return reads.get(pid);
    }

    public synchronized void readLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
        getPages(tid, pid).add(pid);

	    if (hasExclusiveLock(tid, pid)) {
            return;
        }

        while(true) {
            if(!writes.containsKey(pid)) {
                getReads(pid).add(tid);
                return;
            }
        }
    }

    private void incrementCounter(TransactionId tid) {
        xactsCount.put(tid, new Integer(getCount(tid) + 1));
    }

    private int getCount(TransactionId tid) {
        if (!xactsCount.containsKey(tid))
            return 0;
        else
            return xactsCount.get(tid).intValue();
    }

    private boolean canUpgradeLock(TransactionId tid, PageId pid) {
        return reads.get(pid) != null && reads.get(pid).contains(tid) && reads.get(pid).size() == 1;
    }

    public synchronized void writeLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
        getPages(tid, pid).add(pid);

        while(true) {
            if (hasExclusiveLock(tid, pid)) {
                return;
            }

            if (canUpgradeLock(tid, pid)) {
                writes.put(pid, tid);
            }

            if(!writes.containsKey(pid) && getReads(pid).size() == 0) {
                writes.put(pid, tid);
                return;
            }
            else {
                if (getCount(tid) > MAX_COUNT) {
                    try {
                        Database.getBufferPool().transactionComplete(tid);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    throw new TransactionAbortedException();
                }

                incrementCounter(tid);
                Thread.sleep(TIMEOUT);
            }
        }
    }

    public synchronized void readUnlock(TransactionId tid, PageId pid) {
        if (!holdsReadLock(tid, pid))
            return;

    	if (locks.containsKey(tid)) {
    	    locks.get(tid).remove(pid);
    	}

        getReads(pid).remove(tid);
    }

    public synchronized void writeUnlock(TransactionId tid, PageId pid) {
        if (!holdsWriteLock(tid, pid))
            return;

    	if (locks.containsKey(tid)) {
    	    locks.get(tid).remove(pid);
    	}

        writes.remove(pid);
    }

    public boolean holdsReadLock(TransactionId tid, PageId pid) {
        return reads.get(pid) != null && reads.get(pid).contains(tid);
    }

    public boolean holdsWriteLock(TransactionId tid, PageId pid) {
        return writes.get(pid) == tid;
    }

}
