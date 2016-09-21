package prefixSpan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import prefixSpanPrototype.PrefixSpanPrototype;

/**
 * Flink implementation of the PrefixSpan algorithm (DeltaIteration) with additional String field for joining, itemset purification and local processing if applicable.
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class PrefixSpanDeltaLocal {

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
				PrefixSpanDeltaLocal prefixSpan;
				prefixSpan = new PrefixSpanDeltaLocal(args[0],decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
				prefixSpan.run();
			}
		}
	}
	
	private final ExecutionEnvironment env;
	private long threshold ,totalNumberOfSequences;
	private int maxPatternLength;
	private String inputFile;
	private DataSet<Tuple2<Long,int[]>> database;
	private InputDecoder decoder;
	private long maxLocalProjDBSize = 32000000L; //max. number of database items (incl. delimiter) before local processing will be applied instead of MapReduce
	
	/**
	 * Constructor.
	 * @param inputFile URI of the input file
	 * @param decoder to use to decode the input file
	 * @param minSupport as percentage from total number of sequences between 0 and 1
	 * @param maxPatternLength of result patterns
	 * @throws Exception
	 */
	public PrefixSpanDeltaLocal(String inputFile, InputDecoder decoder, double minSupport, int maxPatternLength) throws Exception{
		this.maxPatternLength = maxPatternLength;
		this.inputFile = inputFile;
		this.decoder = decoder;
		env = ExecutionEnvironment.getExecutionEnvironment();
		database = decoder.parse(inputFile, env);
		totalNumberOfSequences = database.count();
		System.out.println("Total number of sequences: " + totalNumberOfSequences);
		threshold = (long)Math.ceil(minSupport * totalNumberOfSequences);
		System.out.println("Threshold: " + threshold);
	}
	
	/**
	 * Sets the max. number of database items (incl. delimiter) before local processing will be applied instead of MapReduce.
	 * Default 32000000 items.
	 * @param maxLocalProjDBSize
	 */
	public void setMaxLocalProjDBSize(long maxLocalProjDBSize){
		this.maxLocalProjDBSize = maxLocalProjDBSize;
	}
	
	/**
	 * Executes the prefixSpan algorithm using deltaIterations and String fields for joining and itemset purification.
	 * @throws Exception
	 */
	public void run() throws Exception{
		//initial workset for delta iteration (prefix, postfix, count, joinPrefix, parentPrefix, postfixLength)
		DataSet<Tuple6<int[],int[],Long,String,String,Long>> initialWorkSet = database.flatMap(new InitialWorkSetExpander());
		
		//initial empty solutionSet for delta iteration (frequent prefix, count, joinPrefix, parentPrefix)
		DataSet<Tuple4<int[],Long,String,String>> initialSolutionSet = env.fromElements(new Tuple4<int[],Long,String,String>(new int[]{0},0L,"","")).filter(new AlwaysFalseFilter());
		
		//initial empty localGroup dataSet which stores the groups for local processing.
		DataSet<Tuple1<String>> localGroups = env.fromElements(new Tuple1<String>(""));
		
		//start the delta iteration
		DeltaIteration<Tuple4<int[],Long,String,String>, Tuple6<int[],int[],Long,String,String,Long>> whileNotEmpty = initialSolutionSet.iterateDelta(initialWorkSet, maxPatternLength, 0);
		
		DataSet<Tuple4<int[],Long,String,String>> solutionSet = null;
		
		if(maxLocalProjDBSize > 0){
			//use pseudoprojection if postfix count of the grouped prefix is <= maxLocalProjDBSize
			
			localGroups = localGroups.union(whileNotEmpty.getWorkset().groupBy(3).sum(5)
					.filter(new PseudoProjectionFilter(maxLocalProjDBSize)).map(new LocalGroupMapper()));
			
			//find the frequent patterns of this iteration step and filter those items which are already local solutions
			solutionSet = whileNotEmpty.getWorkset().groupBy(3).sum(2).andSum(5)
					.filter(new ThresholdFilter2(threshold,maxLocalProjDBSize)).project(0,2,3,4);
		}else{
			//find the frequent patterns of this iteration step
			solutionSet = whileNotEmpty.getWorkset().groupBy(3).sum(2)
					.filter(new ThresholdFilter(threshold)).project(0,2,3,4);
		}
		
		//purifySet for purifying the sequences to reduce the projection size <partenPrefix,newBaseItems>
		DataSet<Tuple2<String,Set<Integer>>> purifySet = solutionSet.groupBy(3).reduceGroup(new PurifyReducer());
				
		//generates a new workset by filtering out the infrequent sequences, purifying the items and projecting
		DataSet<Tuple6<int[],int[],Long,String,String,Long>> workSet = whileNotEmpty.getWorkset().
				joinWithTiny(solutionSet).where(3).equalTo(2).with(new LeftSideOnly()).			//filter infrequent sequences
				joinWithTiny(purifySet).where(4).equalTo(0).with(new Purifier()).				//purify the postfixes
				flatMap(new WorksetFlatMapper());												//project the new workset
		
		//close the delta iteration
		DataSet<Tuple4<int[],Long,String,String>> frequentPatterns = whileNotEmpty.closeWith(solutionSet, workSet);
		
		//calculate the local pseudo projected results
		DataSet<Tuple4<int[],Long,String,String>> localSolutionSet = initialWorkSet.joinWithTiny(localGroups).where(3).equalTo(0)
				.with(new PseudoProjectionJoiner())
				.groupBy(3).reduceGroup(new PseudoProjectionReducer())
				.flatMap(new PseudoProjectionFlatMapper(threshold));
		
		frequentPatterns = frequentPatterns.union(localSolutionSet);
		
		List<Tuple4<int[],Long,String,String>> outputList = frequentPatterns.collect(); //TODO It is currently not supported to create data sinks inside iterations.
		
		//write output to result file or print it to std. out if DummyInputDecoder was used
		 if(outputList.size() > 0){
			DataSet<Tuple4<int[],Long,String,String>> output = env.fromCollection(outputList).sortPartition(0, Order.ASCENDING).setParallelism(1);
			if(decoder instanceof DummyInputDecoder){
				output.map(new OutputMapper()).print();
			}else{
				output.map(new OutputMapper()).writeAsCsv(inputFile+".result.csv").setParallelism(1);
				env.execute();
				System.out.println("Frequent patterns written to: " + inputFile+".result.csv");	
			}
		 }else{
			 System.out.println("No frequent patterns found");
		 }
	}
	
	/**
	 * Expands the database to an initial workset by filtering out infrequent items from the sequences
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class InitialWorkSetExpander implements FlatMapFunction<Tuple2<Long,int[]>,Tuple6<int[],int[],Long,String,String,Long>>{
		private static final long serialVersionUID = 5199181912553925297L;
		@Override
		public void flatMap(Tuple2<Long, int[]> sequence, Collector<Tuple6<int[], int[], Long, String, String, Long>> out) throws Exception {
			Set<Integer> usedPatterns = new HashSet<Integer>();
			usedPatterns.add(0);
			for(int i=0; i<sequence.f1.length; i++){
				if(!usedPatterns.contains(sequence.f1[i])){
					int[] postfix = new int[sequence.f1.length-i-1];
					for(int j=0; j<postfix.length; j++){
						postfix[j] = sequence.f1[j+i+1];
					}
					out.collect(new Tuple6<int[],int[],Long,String,String,Long>(new int[]{sequence.f1[i]},postfix,1L,sequence.f1[i]+" ","",new Long(postfix.length)));
					usedPatterns.add(sequence.f1[i]);
				}
			}
		}
	}
	
	/**
	 * Returns an empty dataset.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class AlwaysFalseFilter implements FilterFunction<Tuple4<int[],Long,String,String>>{
		private static final long serialVersionUID = -6723213013601348207L;
		@Override
		public boolean filter(Tuple4<int[], Long,String,String> arg0) throws Exception {
			return false;
		}
	}
	
	/**
	 * Filters sequences which occur less than threshold.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class ThresholdFilter implements FilterFunction<Tuple6<int[],int[],Long,String,String,Long>>{
		private static final long serialVersionUID = 7122121952386429180L;
		private long threshold;
		public ThresholdFilter(long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple6<int[], int[], Long, String, String, Long> sequence) throws Exception {
			return sequence.f2 >= threshold;
		}
	}
	
	/**
	 * Just returns the joinPrefix of the workset.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class LocalGroupMapper implements MapFunction<Tuple6<int[],int[],Long,String,String,Long>,Tuple1<String>>{
		private static final long serialVersionUID = 7203505120752732820L;
		@Override
		public Tuple1<String> map(Tuple6<int[], int[], Long, String, String, Long> value) throws Exception {
			return new Tuple1<String> (value.f3);
		}
	}
	
	/**
	 * Filters sequences which occur less than threshold or less than maxLocalDBSize
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class ThresholdFilter2 implements FilterFunction<Tuple6<int[],int[],Long,String,String,Long>>{
		private static final long serialVersionUID = 7122121952386429180L;
		private long threshold, maxLocalProjDBSize;
		public ThresholdFilter2(long threshold, long maxLocalProjDBSize){
			this.threshold = threshold;
			this.maxLocalProjDBSize = maxLocalProjDBSize;
		}
		@Override
		public boolean filter(Tuple6<int[], int[], Long, String, String, Long> sequence) throws Exception {
			if(sequence.f2 < threshold ) return false;
			if(sequence.f5 <= maxLocalProjDBSize) return false;
			return true;
		}
	}
	
	/**
	 * Filters out those sequences which could be pseudoprojected and occur less than maxLocalProjDBSize.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class PseudoProjectionFilter implements FilterFunction<Tuple6<int[],int[],Long,String,String,Long>>{
		private static final long serialVersionUID = -4667956556901650445L;
		private long maxLocalProjDBSize;
		public PseudoProjectionFilter(long maxLocalProjDBSize){
			this.maxLocalProjDBSize = maxLocalProjDBSize;
		}
		@Override
		public boolean filter(Tuple6<int[], int[], Long, String, String, Long> sequence) throws Exception {
			return sequence.f5 <= maxLocalProjDBSize;
		}
	}
	
	/**
	 * Generates the pseudo projection database for sequences with the same prefix.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class PseudoProjectionReducer implements GroupReduceFunction<Tuple6<int[],int[],Long,String,String,Long>,Tuple3<int[],List<int[]>,String>>{
		private static final long serialVersionUID = 6768990461202302064L;
		@Override
		public void reduce(Iterable<Tuple6<int[], int[], Long, String, String, Long>> sequences, Collector<Tuple3<int[], List<int[]>, String>> out) throws Exception {
			List<int[]> projDb = new ArrayList<int[]>();
			int[] prefix = new int[0];
			String joinPrefix = "";
			boolean firstRun = true;
			for(Tuple6<int[], int[], Long, String, String, Long> seq : sequences){
				if(firstRun){
					firstRun = false;
					joinPrefix = seq.f3;
					prefix = seq.f0;
				}
				projDb.add(seq.f1);
			}
			if(projDb.size() > 0){
				out.collect(new Tuple3<int[],List<int[]>,String>(prefix,projDb,joinPrefix));
			}
		}
	}
	
	/**
	 * Executes the pseudoprojection if the database fits into RAM.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class PseudoProjectionFlatMapper implements FlatMapFunction<Tuple3<int[], List<int[]>, String>, Tuple4<int[],Long,String,String>>{
		private static final long serialVersionUID = 1968122266820689196L;
		private long threshold; 
		public PseudoProjectionFlatMapper(long threshold){
			this.threshold = threshold;
		}
		@Override
		public void flatMap(Tuple3<int[], List<int[]>, String> pseudo, Collector<Tuple4<int[], Long, String, String>> coll) throws Exception {

			if(pseudo.f1.size() >= threshold){
				
				coll.collect(new Tuple4<int[],Long,String,String>(pseudo.f0,(long) pseudo.f1.size(),pseudo.f2,"local"));
			
				//local prefix span execution
				int lastPrefixItem = pseudo.f0[pseudo.f0.length-1];
			
				PrefixSpanPrototype localPrefixSpan = new PrefixSpanPrototype(pseudo.f1,threshold,0,lastPrefixItem);
				localPrefixSpan.run();
				Map<int[],Long> frequentPatterns = localPrefixSpan.getFrequentPatterns();
			
				//post processing
				for(Map.Entry<int[], Long> cursor : frequentPatterns.entrySet()){
					StringBuilder joinPrefix = new StringBuilder();

					int[] prefix = new int[cursor.getKey().length+pseudo.f0.length-3];
	
					for(int i=0; i<pseudo.f0.length-1; i++){
						prefix[i] = pseudo.f0[i];
						if(cursor.getKey()[i] == 0) joinPrefix.append("| ");
						else joinPrefix.append(cursor.getKey()[i]+" ");
					}
					for(int i=pseudo.f0.length-1, j=pseudo.f0.length; i<prefix.length; i++, j++){
						prefix[i] = cursor.getKey()[j];
						if(cursor.getKey()[j] == 0) joinPrefix.append("| ");
						else joinPrefix.append(cursor.getKey()[j]+" ");
					}
																		//prefix, count, joinPrefix, parentPrefix
					coll.collect(new Tuple4<int[],Long,String,String>(prefix,cursor.getValue(),joinPrefix.toString(),pseudo.f2));
				}
			}
		}
	}
	
	/**
	 * Extracts the frequent base items of the previous prefix
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class PurifyReducer implements GroupReduceFunction<Tuple4<int[],Long,String,String>,Tuple2<String,Set<Integer>>>{
		private static final long serialVersionUID = -9030511257119977874L;
		@Override
		public void reduce(Iterable<Tuple4<int[], Long, String, String>> solutionGroup, Collector<Tuple2<String,Set<Integer>>> coll)
				throws Exception {
			Set<Integer> frequentBaseItems = new HashSet<Integer>();
			String previousPrefix = "";
			boolean firstRun = true;
			for(Tuple4<int[],Long,String,String> sol : solutionGroup){
				if(firstRun){
					previousPrefix = sol.f3;
					firstRun = false;
				}
				frequentBaseItems.add(sol.f0[sol.f0.length-1]);
			}
			coll.collect(new Tuple2<String,Set<Integer>>(previousPrefix,frequentBaseItems));
		}
	}
	
	/**
	 * Returns the left side of the join, filters infrequent sequences.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class LeftSideOnly implements JoinFunction<Tuple6<int[],int[],Long,String,String,Long>,Tuple4<int[],Long,String,String>,Tuple6<int[],int[],Long,String,String,Long>>{
		private static final long serialVersionUID = 5618928544808160900L;
		@Override
		public Tuple6<int[], int[], Long, String, String, Long> join(Tuple6<int[], int[], Long, String, String, Long> first, Tuple4<int[], Long, String, String> second) throws Exception {
			return first;
		}	
	}
	
	/**
	 * Purifies the postfixes by filtering out infrequent baseItems
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class Purifier implements JoinFunction<Tuple6<int[],int[],Long,String,String,Long>,Tuple2<String,Set<Integer>>,Tuple6<int[],int[],Long,String,String,Long>> {
		private static final long serialVersionUID = -3227417891991196718L;
		@Override
		public Tuple6<int[], int[], Long, String, String,Long> join(Tuple6<int[], int[], Long, String, String,Long> workset, Tuple2<String, Set<Integer>> baseItemSet) throws Exception {
			boolean zero = false;
			List<Integer> purifiedPostfixList = new ArrayList<Integer>();
			for(int item : workset.f1){
				if(baseItemSet.f1.contains(item)){
					purifiedPostfixList.add(item);
					zero = false;
				}
				if(!zero && item == 0){
					purifiedPostfixList.add(0);
					zero = true;
				}
			}
			int[] purifiedPostfix = new int[purifiedPostfixList.size()];
			for(int i=0; i<purifiedPostfix.length; i++){
				purifiedPostfix[i] = purifiedPostfixList.get(i);
			}
			return new Tuple6<int[],int[],Long,String,String,Long>(workset.f0,purifiedPostfix,workset.f2,workset.f3,workset.f4,new Long(purifiedPostfix.length));
		}
	}
	
	/**
	 * Creates the new workset by flatmapping the filtered purified old workset
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class WorksetFlatMapper implements FlatMapFunction<Tuple6<int[],int[],Long,String,String,Long>,Tuple6<int[],int[],Long,String,String,Long>> {
		private static final long serialVersionUID = 2858518065305948229L;
		@Override
		public void flatMap(Tuple6<int[], int[], Long, String, String, Long> sequence, Collector<Tuple6<int[], int[], Long, String, String, Long>> out) throws Exception {
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
							StringBuilder prefixString = new StringBuilder();
							for(int s : prefix){
								if(s == 0) prefixString.append("| ");
								else prefixString.append(s+" ");
							}
							out.collect(new Tuple6<int[],int[],Long,String,String,Long>(prefix,postfix,1L,prefixString.toString(),sequence.f3,new Long(postfix.length)));
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
							StringBuilder prefixString = new StringBuilder();
							for(int s : prefix){
								if(s == 0) prefixString.append("| ");
								else prefixString.append(s+" ");
							}
							out.collect(new Tuple6<int[],int[],Long,String,String,Long>(prefix,postfix,1L,prefixString.toString(),sequence.f3,new Long(postfix.length)));
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
									StringBuilder prefixString = new StringBuilder();
									for(int s : prefix){
										if(s == 0) prefixString.append("| ");
										else prefixString.append(s+" ");
									}
									out.collect(new Tuple6<int[],int[],Long,String,String,Long>(prefix,postfix,1L,prefixString.toString(),sequence.f3,new Long(postfix.length)));
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
	 * Returns the first workset.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class PseudoProjectionJoiner implements JoinFunction<Tuple6<int[],int[],Long,String,String,Long>,Tuple1<String>,Tuple6<int[],int[],Long,String,String,Long>>{
		private static final long serialVersionUID = -3270969857423639485L;
		@Override
		public Tuple6<int[], int[], Long, String, String, Long> join(
				Tuple6<int[], int[], Long, String, String, Long> first,
				Tuple1<String> second) throws Exception {
			return first;
		}
	}
	
	/**
	 * Generate output format
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class OutputMapper implements MapFunction<Tuple4<int[],Long,String,String>,Tuple2<String,Long>>{
		private static final long serialVersionUID = -3986454387983543241L;
		@Override
		public Tuple2<String, Long> map(Tuple4<int[], Long, String, String> fpattern) throws Exception {
			return new Tuple2<String,Long>("< "+fpattern.f2+">", fpattern.f1);
		}
	}
}