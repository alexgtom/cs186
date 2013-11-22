package simpledb;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.sql.Timestamp;
import java.util.Date;

public class LockManager {
    private ConcurrentHashMap<PageId, TransactionId> writeMap;
    private ConcurrentHashMap<PageId, ArrayList<TransactionId>> readMap;
    private ConcurrentHashMap<TransactionId, ArrayList<PageId>> locks;
    private ConcurrentHashMap<TransactionId, Integer> counters;
    private static final int TIMEOUT = 10;
    private static final int MAX_COUNT = 25;

    public LockManager() {
        writeMap = new ConcurrentHashMap<PageId, TransactionId>();
        readMap = new ConcurrentHashMap<PageId, ArrayList<TransactionId>>();
	    locks = new ConcurrentHashMap<TransactionId, ArrayList<PageId>>();
	    counters = new ConcurrentHashMap<TransactionId, Integer>();
    }

    public synchronized void addReadLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
	    // adding pid to locks, unsure about placement
	    ArrayList<PageId> pages = new ArrayList<PageId>();
        if (locks.containsKey(tid)) 
	       pages = locks.get(tid);
	    pages.add(pid);
	    locks.put(tid, pages);       

	    if (writeMap.containsKey(pid) && writeMap.get(pid) == tid) {
            ArrayList<TransactionId> transList = readMap.get(pid);
            if(transList == null) {
                transList = new ArrayList<TransactionId>();
                transList.add(tid);
                readMap.put(pid, transList);
            } else if(!transList.contains(tid)) {
                transList.add(tid);
                readMap.put(pid, transList);
            }
            return;
        }

        while(true) {
            if(!writeMap.containsKey(pid)) {
                ArrayList<TransactionId> transList = readMap.get(pid);
                if(transList == null) {
                    transList = new ArrayList<TransactionId>();
                    transList.add(tid);
                    readMap.put(pid, transList);
                    //System.out.println("READ LOCK ADDED BY: " + tid.getId() );
                     break;
                } else if(!transList.contains(tid)) {
                    transList.add(tid);
                    readMap.put(pid, transList);
                    //System.out.println("READ LOCK ADDED BY: " + tid.getId() );
                    break;
                } 
            }

            if (counters.containsKey(tid)) {
                Integer value = counters.get(tid);
                int count = value.intValue() + 1;
                if (count > MAX_COUNT) {
                    if (locks.containsKey(tid)) {
                        ArrayList<PageId> array = locks.get(tid);
                        Object[] pagesArray = array.toArray();
                        for (int i = 0; i < pagesArray.length; i++) {
                            PageId processId = (PageId) pagesArray[i];
                            Database.getBufferPool().releasePage(tid, processId);
                        }
                    }
                    throw new TransactionAbortedException();
                }
                counters.put(tid, new Integer(count));
            } else {
                counters.put(tid, new Integer(1));
            }
            Thread.sleep(TIMEOUT);
        }

    }

    public synchronized void addWriteLock(TransactionId tid, PageId pid) throws InterruptedException, TransactionAbortedException {
    	// adding pid to locks, unsure about placement
    	ArrayList<PageId> pages = new ArrayList<PageId>();
        if (locks.containsKey(tid)) 
    	    pages = locks.get(tid);
    	pages.add(pid);
    	locks.put(tid, pages); 

        if (readMap.containsKey(pid) && readMap.get(pid).contains(tid) && readMap.get(pid).size() == 1) {
            writeMap.put(pid, tid);
        }
        if(writeMap.get(pid) == tid)
            return;

        while(true) {
            ArrayList<TransactionId> transList = readMap.get(pid);
            if(!writeMap.containsKey(pid) && (transList == null || transList.size() == 0)) {
                this.addReadLock(tid,pid);
                writeMap.put(pid, tid);
                break;
            }
            else {
                if (counters.containsKey(tid)) {
                    Integer value = counters.get(tid);
                    int count = value.intValue() + 1;
                    if (count > MAX_COUNT) {
                        if (locks.containsKey(tid)) {
                            ArrayList<PageId> array = locks.get(tid);
                            Object[] pagesArray = array.toArray();
                            for (int i = 0; i < pagesArray.length; i++) {
                                PageId processId = (PageId) pagesArray[i];
                                Database.getBufferPool().releasePage(tid, processId);
                            }
                        }
                        throw new TransactionAbortedException();
                    }
                    counters.put(tid, new Integer(count));
                } else {
                    counters.put(tid, new Integer(1));
                }
                Thread.sleep(TIMEOUT);
            }
        }
    }

    public synchronized void removeReadLock(TransactionId tid, PageId pid) {
    	if (locks.containsKey(tid)) {
    	    ArrayList<PageId> pages = locks.get(tid);
    	    pages.remove(pid);
    	}

        ArrayList<TransactionId> transList = readMap.get(pid);
        transList.remove(tid);
        readMap.put(pid, transList);
        //System.out.println("READ LOCK REMOVED BY: " + tid.getId() );
    }

    public synchronized void removewriteLock(TransactionId tid, PageId pid) {
    	if (locks.containsKey(tid)) {
    	    ArrayList<PageId> pages = locks.get(tid);
    	    pages.remove(pid);
    	}

        writeMap.remove(pid);
        //System.out.println("WRITE LOCK REMOVED BY: " + tid.getId() );
    }

    public boolean holdsReadLock(TransactionId tid, PageId pid) {
        return readMap.get(pid).contains(tid);
    }

    public boolean holdsWriteLock(TransactionId tid, PageId pid) {
        return writeMap.get(pid) == tid;
    }

}
