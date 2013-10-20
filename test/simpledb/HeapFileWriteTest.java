package simpledb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.util.*;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;

public class HeapFileWriteTest extends TestUtil.CreateHeapFile {
    private TransactionId tid;

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void setUp() throws Exception {
        super.setUp();
        tid = new TransactionId();
    }

    @After public void tearDown() throws Exception {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.addTuple()
     */
    @Test public void addTuple() throws Exception {
        // we should be able to add 504 tuples on an empty page.
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            assertEquals(1, empty.numPages());
        }

        // the next 512 additions should live on a new page
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            assertEquals(2, empty.numPages());
        }

        // and one more, just for fun...
        empty.insertTuple(tid, Utility.getHeapTuple(0, 2));
        assertEquals(3, empty.numPages());
    }

    /**
     * Unit test for HeapFile.deleteTuple()
     */
    @Test public void deleteTuple() throws Exception {
        // we should be able to add 504 tuples on an empty page.
        ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
        for (int i = 0; i < 504; ++i) {
            Tuple t = Utility.getHeapTuple(i, 2);
            tupleList.add(t);
            empty.insertTuple(tid, t);
            assertEquals(1, empty.numPages());
        }

        for (int i = 0; i < tupleList.size(); ++i) {
            empty.deleteTuple(tid, tupleList.get(i));
        }
        HeapPage p = (HeapPage) empty.readPage(new HeapPageId(empty.getId(), 0));
        assertEquals(504, p.getNumEmptySlots());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileWriteTest.class);
    }
}

