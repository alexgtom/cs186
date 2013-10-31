package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numBuckets;
    private int min;
    private int max;
    private int ntups;
    private int[] bucketHeight;
    private double interval;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        this.ntups = 0;
        this.bucketHeight = new int[buckets];
        this.interval = (double) Math.abs(max - min) / buckets;
        this.numBuckets = buckets;
    }

    private int getBucketNum(int v) {
        int bucketNum = (int) ((v - min) / interval);
        if (bucketNum == numBuckets && bucketNum > 0)
            return bucketNum - 1;
        else
            return bucketNum;
    }

    private double getRight(int bucketNum) {
        return (bucketNum + 1) * interval;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        bucketHeight[getBucketNum(v)]++;
        ntups++;
    }

    private double selectivityEq(int v) {
        if (v > max)
            return 0.0;
        if (v < min)
            return 0.0;
        return bucketHeight[getBucketNum(v)] / (Math.max(1, interval) * ntups);
    }

    private double selectivityGt(int v) {
        if (v > max)
            return 0.0;
        if (v < min)
            return 1.0;
        int bucketNum = getBucketNum(v);
        double rightBuckets = 0.0f;
        for(int i = bucketNum + 1; i < numBuckets; i++) {
            rightBuckets += (double) bucketHeight[i] / ntups;
        }

        return bucketHeight[bucketNum] * (getRight(bucketNum) - v) / (interval * ntups) + rightBuckets;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        if (op == Predicate.Op.EQUALS)
            return selectivityEq(v);
        if (op == Predicate.Op.GREATER_THAN)
            return selectivityGt(v);
        if (op == Predicate.Op.LESS_THAN)
            return 1.0 - selectivityGt(v);
        if (op == Predicate.Op.LESS_THAN_OR_EQ)
            return 1.0 - selectivityGt(v);
        if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            double value = selectivityGt(v) + selectivityEq(v);
            if (value > 1.0)
                return 1.0;
            if (value < 0.0)
                return 0.0;
            return value;
        }
        if (op == Predicate.Op.NOT_EQUALS)
            return 1.0 - selectivityEq(v);

        // invalid operator
        return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return null;
    }
}
