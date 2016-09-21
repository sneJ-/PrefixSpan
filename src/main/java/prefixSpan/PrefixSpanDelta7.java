package prefixSpan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

/**
 * Flink implementation of the PrefixSpan algorithm (DeltaIteration) with additional String field for joining and itemset purification.
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class PrefixSpanDelta7 {

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
				PrefixSpanDelta7 prefixSpan;
				prefixSpan = new PrefixSpanDelta7(args[0],decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
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
	
	/**
	 * Constructor.
	 * @param inputFile URI of the input file
	 * @param decoder to use to decode the input file
	 * @param minSupport as percentage from total number of sequences between 0 and 1
	 * @param maxPatternLength of result patterns
	 * @throws Exception
	 */
	public PrefixSpanDelta7(String inputFile, InputDecoder decoder, double minSupport, int maxPatternLength) throws Exception{
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
	 * Executes the prefixSpan algorithm using deltaIterations and String fields for joining and itemset purification.
	 * @throws Exception
	 */
	public void run() throws Exception{
		//initial workset for delta iteration (prefix, postfix, count, joinPrefix, parentPrefix)
		DataSet<Tuple5<int[],int[],Long,String,String>> initialWorkSet = database.flatMap(new InitialWorkSetExpander());
		
		//initial empty solutionSet for delta iteration (frequent prefix, count, joinPrefix, parentPrefix)
		DataSet<Tuple4<int[],Long,String,String>> initialSolutionSet = env.fromElements(new Tuple4<int[],Long,String,String>(new int[]{0},0L,"","")).filter(new AlwaysFalseFilter());
		
		//start the delta iteration
		DeltaIteration<Tuple4<int[],Long,String,String>, Tuple5<int[],int[],Long,String,String>> whileNotEmpty = initialSolutionSet.iterateDelta(initialWorkSet, maxPatternLength, 0);
		
		//find the frequent patterns of this iteration step
		DataSet<Tuple4<int[],Long,String,String>> solutionSet = whileNotEmpty.getWorkset().groupBy(3).sum(2).filter(new ThresholdFilter(threshold)).project(0,2,3,4);
		
		//purifySet for purifying the sequences to reduce the projection size <partenPrefix,newBaseItems>
		DataSet<Tuple2<String,Set<Integer>>> purifySet = solutionSet.groupBy(3).reduceGroup(new PurifyReducer());
				
		//generates a new workset by filtering out the infrequent sequences, purifying the items and projecting
		DataSet<Tuple5<int[],int[],Long,String,String>> workSet = whileNotEmpty.getWorkset().
				joinWithTiny(solutionSet).where(3).equalTo(2).with(new LeftSideOnly()).			//filter infrequent sequences
				joinWithTiny(purifySet).where(4).equalTo(0).with(new Purifier()).				//purify the postfixes
				flatMap(new WorksetFlatMapper());												//project the new workset
		
		//close the delta iteration
		DataSet<Tuple4<int[],Long,String,String>> frequentPatterns = whileNotEmpty.closeWith(solutionSet, workSet);
		
		List<Tuple4<int[],Long,String,String>> outputList = frequentPatterns.collect();
		
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
	private class InitialWorkSetExpander implements FlatMapFunction<Tuple2<Long,int[]>,Tuple5<int[],int[],Long,String,String>>{
		private static final long serialVersionUID = 5199181912553925297L;
		@Override
		public void flatMap(Tuple2<Long, int[]> sequence, Collector<Tuple5<int[], int[], Long, String, String>> out) throws Exception {
			Set<Integer> usedPatterns = new HashSet<Integer>();
			usedPatterns.add(0);
			for(int i=0; i<sequence.f1.length; i++){
				if(!usedPatterns.contains(sequence.f1[i])){
					int[] postfix = new int[sequence.f1.length-i-1];
					for(int j=0; j<postfix.length; j++){
						postfix[j] = sequence.f1[j+i+1];
					}
					out.collect(new Tuple5<int[],int[],Long,String,String>(new int[]{sequence.f1[i]},postfix,1L,sequence.f1[i]+" ",""));
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
	private class ThresholdFilter implements FilterFunction<Tuple5<int[],int[],Long,String,String>>{
		private static final long serialVersionUID = 7122121952386429180L;
		private long threshold;
		public ThresholdFilter(long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple5<int[], int[], Long, String, String> sequence) throws Exception {
			return sequence.f2 >= threshold;
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
	private class LeftSideOnly implements JoinFunction<Tuple5<int[],int[],Long,String,String>,Tuple4<int[],Long,String,String>,Tuple5<int[],int[],Long,String,String>>{
		private static final long serialVersionUID = 5618928544808160900L;
		@Override
		public Tuple5<int[], int[], Long, String, String> join(Tuple5<int[], int[], Long, String, String> first, Tuple4<int[], Long, String, String> second) throws Exception {
			return first;
		}	
	}
	
	/**
	 * Purifies the postfixes by filtering out infrequent baseItems
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class Purifier implements JoinFunction<Tuple5<int[],int[],Long,String,String>,Tuple2<String,Set<Integer>>,Tuple5<int[],int[],Long,String,String>> {
		private static final long serialVersionUID = -3227417891991196718L;
		@Override
		public Tuple5<int[], int[], Long, String, String> join(Tuple5<int[], int[], Long, String, String> workset, Tuple2<String, Set<Integer>> baseItemSet) throws Exception {
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
			return new Tuple5<int[],int[],Long,String,String>(workset.f0,purifiedPostfix,workset.f2,workset.f3,workset.f4);
		}
	}
	
	/**
	 * Creates the new workset by flatmapping the filtered purified old workset
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class WorksetFlatMapper implements FlatMapFunction<Tuple5<int[],int[],Long,String,String>,Tuple5<int[],int[],Long,String,String>> {
		private static final long serialVersionUID = 2858518065305948229L;
		@Override
		public void flatMap(Tuple5<int[], int[], Long, String, String> sequence, Collector<Tuple5<int[], int[], Long, String, String>> out) throws Exception {
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
							out.collect(new Tuple5<int[],int[],Long,String,String>(prefix,postfix,1L,prefixString.toString(),sequence.f3));
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
							out.collect(new Tuple5<int[],int[],Long,String,String>(prefix,postfix,1L,prefixString.toString(),sequence.f3));
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
									out.collect(new Tuple5<int[],int[],Long,String,String>(prefix,postfix,1L,prefixString.toString(),sequence.f3));
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
