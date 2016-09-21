package prefixSpan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * Flink implementation of the PrefixSpan algorithm (DeltaIteration) with additional String field for joining and itemset purification.
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class PrefixSpanDelta6 {

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
				PrefixSpanDelta6 prefixSpan;
				if(decoder instanceof DummyInputDecoder){
					prefixSpan = new PrefixSpanDelta6(null,decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
				}else{
					prefixSpan = new PrefixSpanDelta6(args[0],decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
				}
				prefixSpan.run();
			}
		}
	}
	
	private final ExecutionEnvironment env;
	private long threshold ,totalNumberOfSequences;
	private int maxPatternLength;
	private String inputFile;
	private DataSet<Tuple2<Long,int[]>> database;
	
	/**
	 * Constructor.
	 * @param inputFile URI of the input file
	 * @param decoder to use to decode the input file
	 * @param minSupport as percentage from total number of sequences between 0 and 1
	 * @param maxPatternLength of result patterns
	 * @throws Exception
	 */
	public PrefixSpanDelta6(String inputFile, InputDecoder decoder, double minSupport, int maxPatternLength) throws Exception{
		this.maxPatternLength = maxPatternLength;
		this.inputFile = inputFile;
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
		//length 1 frequent base items
		DataSet<Integer> length1FrequentItems = database.flatMap(new BaseItemExpander()).groupBy(0).sum(1).filter(new BaseItemThresholdFilter(threshold)).map(new BaseItemMapper());
		List<Integer> baseItems = length1FrequentItems.collect();
		
		//purified initial workset for delta iteration (prefix, postfix, count, joinPrefix)
		DataSet<Tuple4<int[],int[],Long,String>> initialWorkSet = database.flatMap(new InitialWorkSetExpander(baseItems));
		
		//initial empty solutionSet for delta iteration (frequent pattern, frequency, joinPrefix)
		DataSet<Tuple3<int[],Long,String>> initialSolutionSet = env.fromElements(new Tuple3<int[],Long,String>(new int[]{0},0L,"")).filter(new AlwaysFalseFilter());
		
		//start the delta iteration
		DeltaIteration<Tuple3<int[],Long,String>, Tuple4<int[], int[], Long,String>> whileNotEmpty = initialSolutionSet.iterateDelta(initialWorkSet, maxPatternLength, 0);
		
		//find the recent frequent patterns of this iteration step
		DataSet<Tuple3<int[],Long,String>> solutionSet = whileNotEmpty.getWorkset().groupBy(3).sum(2).filter(new ThresholdFilter(threshold)).project(0,2,3);
		
		//generate a new workset by filtering out the infrequent worksets and projecting the others
		DataSet<Tuple4<int[],int[],Long,String>> workSet = whileNotEmpty.getWorkset().join(solutionSet).where(3).equalTo(2).with(new WorksetFlatJoiner());
		
		//close the delta iteration
		DataSet<Tuple3<int[],Long,String>> frequentPatterns = whileNotEmpty.closeWith(solutionSet, workSet);
		
		List<Tuple3<int[],Long,String>> outputList = frequentPatterns.collect();
		
		//write output to result file or print it to std. out if dummy encoder was used
		 if(outputList.size() > 0){
			DataSet<Tuple3<int[],Long,String>> output = env.fromCollection(outputList).sortPartition(0, Order.ASCENDING).setParallelism(1);
			if(inputFile == null){ //dummyEncoder
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
	 * Expands the database for base item extraction
	 * @author Jens Röwekamp, Tianlong du
	 *
	 */
	public class BaseItemExpander implements FlatMapFunction<Tuple2<Long, int[]>, Tuple2<Integer,Long>> {
		private static final long serialVersionUID = 2631435511600829511L;
		@Override
		public void flatMap(Tuple2<Long, int[]> sequence, Collector<Tuple2<Integer,Long>> out) throws Exception {
			Set<Integer> usedPatterns = new HashSet<Integer>();
			usedPatterns.add(0);
			for(int i=0; i<sequence.f1.length; i++){
				if(!usedPatterns.contains(sequence.f1[i])){
					out.collect(new Tuple2<Integer,Long>(sequence.f1[i],1L));
					usedPatterns.add(sequence.f1[i]);
				}
			}
		}
	}
	
	/**
	 * Filters infrequent baseItems
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class BaseItemThresholdFilter implements FilterFunction<Tuple2<Integer,Long>>{
		private static final long serialVersionUID = -5105912430351516825L;
		private Long threshold;
		public BaseItemThresholdFilter(Long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple2<Integer, Long> arg0) throws Exception {
			return arg0.f1 >= threshold;
		}
	}
	
	/**
	 * Extracts the frequent baseItems
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class BaseItemMapper implements MapFunction<Tuple2<Integer,Long>,Integer>{
		private static final long serialVersionUID = -6143546380005636970L;
		@Override
		public Integer map(Tuple2<Integer, Long> arg0) throws Exception {
			return arg0.f0;
		}
	}
	
	/**
	 * Expands the database to an initial workset by filtering out infrequent items from the sequences
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class InitialWorkSetExpander implements FlatMapFunction<Tuple2<Long,int[]>,Tuple4<int[],int[],Long,String>>{
		private static final long serialVersionUID = 5199181912553925297L;
		private List<Integer> baseItems;
		public InitialWorkSetExpander(List<Integer> baseItems){
			this.baseItems = baseItems;
		}
		@Override
		public void flatMap(Tuple2<Long, int[]> sequence, Collector<Tuple4<int[], int[], Long, String>> out)
				throws Exception {
			Set<Integer> usedPatterns = new HashSet<Integer>();
			usedPatterns.add(0);
			for(int i=0; i<sequence.f1.length; i++){
				boolean zero = false;
				if(!usedPatterns.contains(sequence.f1[i])){
					//purify the postfix, filter out infrequent items
					List<Integer> postfix = new ArrayList<Integer>();
					for(int j=0; j<sequence.f1.length-i-1; j++){
						if(baseItems.contains(sequence.f1[j+i+1])){
							postfix.add(sequence.f1[j+i+1]);
							zero = false;
						}
						if(!zero && sequence.f1[j+i+1] == 0){ //no duplicate delimiting zeros
							postfix.add(0);
							zero = true;
						}
					}
					int[] postfixArray = new int[postfix.size()];
					for(int j=0; j<postfixArray.length; j++){
						postfixArray[j] = postfix.get(j);
					}
					out.collect(new Tuple4<int[],int[],Long,String>(new int[]{sequence.f1[i]},postfixArray,1L,""+sequence.f1[i]));
					usedPatterns.add(sequence.f1[i]);
				}
			}
		}
	}
	
	/**
	 * Returns an empty dataset.
	 * @author Jens Röwwekamp, Tianlong Du
	 *
	 */
	private class AlwaysFalseFilter implements FilterFunction<Tuple3<int[],Long,String>>{
		private static final long serialVersionUID = -6723213013601348207L;
		@Override
		public boolean filter(Tuple3<int[], Long,String> arg0) throws Exception {
			return false;
		}
	}
	
	/**
	 * Filters sequences which occur less than threshold.
	 * @author Jens RÃ¶wekamp, Tianlong Du
	 *
	 */
	private class ThresholdFilter implements FilterFunction<Tuple4<int[],int[],Long,String>>{
		private static final long serialVersionUID = 7122121952386429180L;
		private long threshold;
		public ThresholdFilter(long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple4<int[], int[], Long, String> sequence) throws Exception {
			return sequence.f2 >= threshold;
		}
	}
	
	/**
	 * Creates the new workset by flat joining the old workset and solutionset
	 * @author Jens RÃ¶wekamp, Tianlong Du
	 *
	 */
	private class WorksetFlatJoiner implements FlatJoinFunction<Tuple4<int[],int[],Long,String>,Tuple3<int[],Long,String>,Tuple4<int[],int[],Long,String>> {
		private static final long serialVersionUID = 2858518065305948229L;
		@Override
		public void join(Tuple4<int[], int[], Long,String> sequence, Tuple3<int[], Long,String> solution, Collector<Tuple4<int[], int[], Long,String>> out) throws Exception {
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
								prefixString.append(s);
							}
							out.collect(new Tuple4<int[],int[],Long,String>(prefix,postfix,1L,prefixString.toString()));
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
								prefixString.append(s);
							}
							out.collect(new Tuple4<int[],int[],Long,String>(prefix,postfix,1L,prefixString.toString()));
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
										prefixString.append(s);
									}
									out.collect(new Tuple4<int[],int[],Long,String>(prefix,postfix,1L,prefixString.toString()));
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
	private class OutputMapper implements MapFunction<Tuple3<int[],Long,String>,Tuple2<String,Long>>{
		private static final long serialVersionUID = -3986454387983543241L;
		@Override
		public Tuple2<String, Long> map(Tuple3<int[], Long, String> fpattern) throws Exception {
			StringBuilder out = new StringBuilder("<");
			for(int i : fpattern.f0){
				if(i == 0) out.append("|");
				else out.append(" "+i+" ");
			}
			out.append(">");
			return new Tuple2<String,Long>(out.toString(), fpattern.f1);
		}
	}
}
