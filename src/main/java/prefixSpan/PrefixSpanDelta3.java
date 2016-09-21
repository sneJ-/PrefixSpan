package prefixSpan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * Flink implementation of the PrefixSpan algorithm (DeltaIteration).
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class PrefixSpanDelta3 {

	/**
	 * Main Function to execute the algorithm from command line.
	 * Interprets the command line arguments and executes the algorithm.
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		if(args.length != 4){
			System.err.println("Error - Arguments");
			System.err.println("PrefixSpanDelta.java inputFile decoder minSupport maxPatternLength");
			System.err.println("decoder types: plain (PlainInputDecoder), bigBench (BigBenchInputDecoder), ibm (IBM_DataGeneratorInputDecoder) or dummy (DummyInputDecoder)");
		} else if(Double.parseDouble(args[2]) > 1 || Double.parseDouble(args[2]) < 0){
			System.err.println("Error - minSupport");
			System.err.println("PrefixSpanDelta.java inputFile decoder minSupport maxPatternLength");
			System.err.println("minSupport needs to be between 0 and 1");
		} else if(Integer.parseInt(args[3]) < 1){
			System.err.println("Error - maxPatternLength");
			System.err.println("PrefixSpanDelta.java inputFile decoder minSupport maxPatternLength");
			System.err.println("maxPatternLength needs to be higher than 0");
		} else{
			InputDecoder decoder = null;
			switch(args[1]){
			case "plain": decoder = new PlainInputDecoder(); break;
			case "bigBench": decoder = new BigBenchInputDecoder(); break;
			case "ibm": decoder = new IBM_DataGeneratorInputDecoder(); break;
			case "dummy": decoder = new DummyInputDecoder(); break;
			default: System.err.println("Error - decoder");
				System.err.println("PrefixSpanDelta.java inputFile decoder minSupport maxPatternLength"); 
				System.err.println("decoder types: plain (PlainInputDecoder), bigBench (BigBenchInputDecoder), ibm (IBM_DataGeneratorInputDecoder) or dummy (DummyInputDecoder)");
			}
			if(decoder != null){
				PrefixSpanDelta3 prefixSpan;
				if(decoder instanceof DummyInputDecoder){
					prefixSpan = new PrefixSpanDelta3(null,decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
				}else{
					prefixSpan = new PrefixSpanDelta3(args[0],decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
				}
				prefixSpan.run();
			}
		}
	}
	
	private final ExecutionEnvironment env;
	private long threshold ,totalNumberOfSequences;
	private int maxPatternLength;
	private DataSet<Tuple2<Long,int[]>> database;
	
	/**
	 * Constructor.
	 * @param inputFile URI of the input file
	 * @param decoder to use to decode the input file
	 * @param minSupport as percentage from total number of sequences between 0 and 1
	 * @param maxPatternLength of result patterns
	 * @throws Exception
	 */
	public PrefixSpanDelta3(String inputFile, InputDecoder decoder, double minSupport, int maxPatternLength) throws Exception{
		this.maxPatternLength = maxPatternLength;
		env = ExecutionEnvironment.getExecutionEnvironment();
		database = decoder.parse(inputFile, env);
		totalNumberOfSequences = database.count();
		System.out.println("Total number of sequences: " + totalNumberOfSequences);
		threshold = (long)Math.ceil(minSupport * totalNumberOfSequences);
		System.out.println("Threshold: " + threshold);
	}
	
	/**
	 * Executes the prefixSpan algorithm using deltaIterations.
	 * @throws Exception
	 */
	public void run() throws Exception{
		//compute the length 1 frequent base items (frequent pattern, frequency)
		DataSet<Tuple2<int[],Long>> frequentPatterns = database.flatMap(new BaseItemFlatMapper()).groupBy(0).sum(1).filter(new BaseItemFilter(threshold));
		List<Tuple2<int[],Long>> frequentBaseItems = frequentPatterns.collect();
		frequentPatterns.print();
				
		//for each base item find frequent sub patterns and add them to frequentPatterns
		for(Tuple2<int[],Long> baseItem : frequentBaseItems){
			//initial workset for delta iteration (prefix, postfix, count)
			DataSet<Tuple3<int[],int[],Long>> initialWorkSet = database.flatMap(new InitialWorkSetExpander(baseItem.f0));
			
			//initial empty solutionSet for delta iteration
			DataSet<Tuple2<int[],Long>> initialSolutionSet = env.fromElements(new Tuple2<int[],Long>(new int[]{0},0L)).filter(new AlwaysFalseFilter());
			
			//start the delta iteration
			DeltaIteration<Tuple2<int[],Long>, Tuple3<int[], int[], Long>> whileNotEmpty = initialSolutionSet.iterateDelta(initialWorkSet, maxPatternLength, 0);
			
			//find the recent frequent patterns of this iteration step
			DataSet<Tuple2<int[],Long>> solutionSet = whileNotEmpty.getWorkset().groupBy(0).sum(2).filter(new ThresholdFilter(threshold)).project(0,2);
			
			//generate a new workset by filtering out the infrequent worksets and projecting the others
			DataSet<Tuple3<int[],int[],Long>> workSet = whileNotEmpty.getWorkset().join(solutionSet).where(0).equalTo(0).with(new WorksetFlatJoiner());
			
			//close the delta iteration
			DataSet<Tuple2<int[],Long>> frequentSubPatterns = whileNotEmpty.closeWith(solutionSet, workSet);
			
//			frequentPatterns = frequentPatterns.union(whileNotEmpty.closeWith(solutionSet, workSet)); //TODO resolve when flink bug is resolved.
			
			//print frequent sub patterns as union is not possible.
			frequentSubPatterns.print();
		}
	}
	
	/**
	 * Extracts the base items of the database.
	 * No delimiting 0s are returned.
	 * @author Jens R�ekamp, Tianlong Du
	 *
	 */
	private class BaseItemFlatMapper implements FlatMapFunction<Tuple2<Long,int[]>, Tuple2<int[],Long>> {
		private static final long serialVersionUID = 8916622873868433406L;
		public void flatMap(Tuple2<Long,int[]> database, Collector<Tuple2<int[],Long>> coll) throws Exception{
			//container for unique items
			Set<Integer> uniqueItems = new HashSet<Integer>();
			//add every item of sequence to set
			for(Integer item : database.f1){
				uniqueItems.add(item);
			}
			//construct the result
			for(Integer uniqueItem : uniqueItems){
				//no delimiting 0 shall be counted
				if(uniqueItem != 0)	coll.collect(new Tuple2<int[],Long>(new int[]{uniqueItem},1L));
			}
		}
	}
	
	/**
	 * Filters base items which occur less than threshold.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class BaseItemFilter implements FilterFunction<Tuple2<int[],Long>>{
		private static final long serialVersionUID = 7996269067554680714L;
		private Long threshold;
		public BaseItemFilter(Long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple2<int[], Long> baseItem) throws Exception {
			return baseItem.f1 >= threshold;
		}
	}
	
	/**
	 * Expands the database to an initial workset.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class InitialWorkSetExpander implements FlatMapFunction<Tuple2<Long,int[]>,Tuple3<int[],int[],Long>>{
		private static final long serialVersionUID = 5199181912553925297L;
		private int baseItem;
		public InitialWorkSetExpander(int[] baseItem){
			this.baseItem = baseItem[0];
		}
		@Override
		public void flatMap(Tuple2<Long, int[]> sequence, Collector<Tuple3<int[], int[], Long>> out)
				throws Exception {
			Set<Integer> usedPatterns = new HashSet<Integer>();
			boolean insideFirstItemSet = true;
			//find first occurrence of baseItem
			int firstOccurrencePointer = 0;
			for(int item : sequence.f1){
				if(item == baseItem) break;
				firstOccurrencePointer++;
			}
			//create the initial workset
			for(int i=firstOccurrencePointer+1; i<sequence.f1.length; i++){
				if(sequence.f1[i] == 0){
					insideFirstItemSet = false;
				}else{
					if(insideFirstItemSet){
						//add pattern as -pattern if not already in usedPatterns
						if(!usedPatterns.contains(-sequence.f1[i])){ //(acd)(cd)(ef) --> (_cd)(cd)(ef) --> ac, ad
							int[] postfix = new int[sequence.f1.length-i-1];
							for(int j=0; j<postfix.length; j++){
								postfix[j] = sequence.f1[i+j+1];
							}
							out.collect(new Tuple3<int[],int[],Long>(new int[]{baseItem,sequence.f1[i]},postfix,1L));
							usedPatterns.add(-sequence.f1[i]);
						}
					}else{
						if(!usedPatterns.contains(sequence.f1[i])){ //a(abc)(cd) --> (abc)(cd) --> a a, a b, a c, a d
							int[] postfix = new int[sequence.f1.length-i-1];
							for(int j=0; j<postfix.length; j++){
								postfix[j] = sequence.f1[i+j+1];
							}
							out.collect(new Tuple3<int[],int[],Long>(new int[]{baseItem,0,sequence.f1[i]},postfix,1L));
							usedPatterns.add(sequence.f1[i]);
						}
						if(sequence.f1[i] == baseItem){  //a(abc)(cd) --> (abc)(cd) --> ab, ac
							for(int k=i+1; k<sequence.f1.length; k++){
								if(sequence.f1[k]==0) break;
								if(!usedPatterns.contains(-sequence.f1[k])){
									int[] postfix = new int[sequence.f1.length-k-1];
									for(int j=0; j<postfix.length; j++){
										postfix[j] = sequence.f1[k+j+1];
									}
									out.collect(new Tuple3<int[],int[],Long>(new int[]{baseItem,sequence.f1[k]},postfix,1L));
									usedPatterns.add(-sequence.f1[k]);
								}
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * Returns an empty dataset.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class AlwaysFalseFilter implements FilterFunction<Tuple2<int[],Long>>{
		private static final long serialVersionUID = -6723213013601348207L;
		@Override
		public boolean filter(Tuple2<int[], Long> arg0) throws Exception {
			return false;
		}
	}
	
	/**
	 * Filters sequences which occur less than threshold.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class ThresholdFilter implements FilterFunction<Tuple3<int[],int[],Long>>{
		private static final long serialVersionUID = 7122121952386429180L;
		private long threshold;
		public ThresholdFilter(long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple3<int[], int[], Long> sequence) throws Exception {
			return sequence.f2 >= threshold;
		}
	}
	
	/**
	 * Creates the new workset by flat joining the old workset and solutionset
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class WorksetFlatJoiner implements FlatJoinFunction<Tuple3<int[],int[],Long>,Tuple2<int[],Long>,Tuple3<int[],int[],Long>> {
		private static final long serialVersionUID = 2858518065305948229L;
		@Override
		public void join(Tuple3<int[], int[], Long> sequence, Tuple2<int[], Long> solution, Collector<Tuple3<int[], int[], Long>> out) throws Exception {
			Set<Integer> usedPatterns = new HashSet<Integer>();
			boolean insideFirstItemSet = true;
			int pattern = sequence.f0[sequence.f0.length-1]; //last item of the prefix is relevant
			
			//generate the new workset
			for(int i=0; i<sequence.f1.length; i++){
				if(sequence.f1[i] == 0){
					insideFirstItemSet = false;
				}else{
					if(insideFirstItemSet){
						//add pattern as -pattern if not already in usedPatterns
						if(!usedPatterns.contains(-sequence.f1[i])){ //a --> cd)(cd)(ef) --> ac, ad
							int[] postfix = new int[sequence.f1.length-i-1];
							for(int j=0; j<postfix.length; j++){
								postfix[j] = sequence.f1[i+j+1];
							}
							int[] prefix = new int[sequence.f0.length+1];
							for(int j=0; j<sequence.f0.length; j++){
								prefix[j] = sequence.f0[j];
							}
							prefix[prefix.length-1] = sequence.f1[i];
							out.collect(new Tuple3<int[],int[],Long>(prefix,postfix,1L));
							usedPatterns.add(-sequence.f1[i]);
						}
					}else{
						if(!usedPatterns.contains(sequence.f1[i])){ //a --> (abc)(cd) --> a a, a b, a c, a d
							int[] postfix = new int[sequence.f1.length-i-1];
							for(int j=0; j<postfix.length; j++){
								postfix[j] = sequence.f1[i+j+1];
							}
							int[] prefix = new int[sequence.f0.length+2];
							for(int j=0; j<sequence.f0.length; j++){
								prefix[j] = sequence.f0[j];
							}
							prefix[prefix.length-2] = 0;
							prefix[prefix.length-1] = sequence.f1[i];
							out.collect(new Tuple3<int[],int[],Long>(prefix,postfix,1L));
							usedPatterns.add(sequence.f1[i]);
						}
						if(sequence.f1[i] == pattern){  //a --> (abc)(cd) --> ab, ac
							for(int k=i+1; k<sequence.f1.length; k++){
								if(sequence.f1[k]==0) break;
								if(!usedPatterns.contains(-sequence.f1[k])){
									int[] postfix = new int[sequence.f1.length-k-1];
									for(int j=0; j<postfix.length; j++){
										postfix[j] = sequence.f1[k+j+1];
									}
									int[] prefix = new int[sequence.f0.length+1];
									for(int j=0; j<sequence.f0.length; j++){
										prefix[j] = sequence.f0[j];
									}
									prefix[prefix.length-1] = sequence.f1[k];
									out.collect(new Tuple3<int[],int[],Long>(prefix,postfix,1L));
									usedPatterns.add(-sequence.f1[k]);
								}
							}
						}
					}
				}
			}
		}
	}
}
