package simpledb;

import simpledb.TestUtil.LockGrabber;
import simpledb.HeapPageId;

import java.util.*;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;

public class LockManagerTest {
  @Test public void testSmallGraph() throws Exception {
      LockManager lm = new LockManager();
      TransactionId tid1 = new TransactionId();
      TransactionId tid2 = new TransactionId();
      PageId pid1 = new HeapPageId(0, 1);
      PageId pid2 = new HeapPageId(0, 2);
      
      lm.writeLock(tid1, pid1);
      lm.writeLock(tid2, pid2);
      lm.writeLock(tid1, pid2);
      lm.printGraph();
      boolean thrown = false;
      try {
        lm.writeLock(tid2, pid2);
        lm.printGraph();
      } catch (TransactionAbortedException e) {
          System.out.println("thrown");
        thrown = true;
      }

      assertTrue(thrown);
  }

  @Test public void testBiggerGraph() throws Exception {
      LockManager lm = new LockManager();
      TransactionId tid1 = new TransactionId();
      TransactionId tid2 = new TransactionId();
      TransactionId tid3 = new TransactionId();
      TransactionId tid4 = new TransactionId();
      TransactionId tid5 = new TransactionId();
      PageId pid1 = new HeapPageId(0, 1);
      PageId pid2 = new HeapPageId(0, 2);
      PageId pid3 = new HeapPageId(0, 3);
      PageId pid4 = new HeapPageId(0, 4);
      PageId pid5 = new HeapPageId(0, 5);

      // create loop
      lm.writeLock(tid1, pid1);
      lm.writeLock(tid2, pid2);
      lm.writeLock(tid3, pid3);
      lm.writeLock(tid4, pid4);

      lm.writeLock(tid1, pid2);
      lm.writeLock(tid2, pid3);
      lm.writeLock(tid3, pid4);

      lm.writeLock(tid1, pid5);

      // do some random reads
      //lm.readLock(tid1, pid2);
      //lm.readLock(tid2, pid4);
      //lm.readLock(tid3, pid3);
      //lm.readLock(tid4, pid1);

      lm.printGraph();
      boolean thrown = false;
      try {
        lm.writeLock(tid4, pid1);
      } catch (TransactionAbortedException e) {
          System.out.println("thrown");
        thrown = true;
      }

      assertTrue(thrown);
  }

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(LockManagerTest.class);
  }

}

