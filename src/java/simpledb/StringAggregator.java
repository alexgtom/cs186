package simpledb;

import java.util.*;
import java.lang.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;

    private HashMap<Field, ArrayList<Field>> groupsMap;
    private ArrayList<Field> nogroupsList;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;

        if(gbfield == Aggregator.NO_GROUPING)
            nogroupsList = new ArrayList<Field>();
        else
            this.groupsMap = new HashMap<Field, ArrayList<Field>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(this.gbfield == Aggregator.NO_GROUPING)
            nogroupsList.add(tup.getField(this.afield));

        else{
            Field theGroup = tup.getField(this.gbfield);
            Field theValue = tup.getField(this.afield);

            ArrayList<Field> valuesList = groupsMap.get(theGroup);
            if(valuesList != null)
                valuesList.add(theValue);
            else{
                valuesList = new ArrayList<Field>();
                valuesList.add(theValue);
            }
            groupsMap.put(theGroup,valuesList);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        if(this.gbfield == Aggregator.NO_GROUPING){
            int count = nogroupsList.size();

            TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(count));

            ArrayList<Tuple> tupArr = new ArrayList<Tuple>();
            tupArr.add(t);
            return new TupleIterator(td, tupArr);
        }

        else{
            ArrayList<Tuple> tupArr = new ArrayList<Tuple>();
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

            Set keys = groupsMap.keySet();
            Iterator<Field> keysIterator = keys.iterator();
            while(keysIterator.hasNext()){
                Field f = keysIterator.next();
                ArrayList valuesList = groupsMap.get(f);
                int count = valuesList.size();

                Tuple t = new Tuple(td);
                t.setField(0, f);
                t.setField(1, new IntField(count));
                tupArr.add(t);
            }

            return new TupleIterator(td, tupArr);
        }
    }

}
