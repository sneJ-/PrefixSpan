package prefixSpan;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

/**
 * Flink implementation of the PrefixSpan algorithm (DeltaIteration).
 * @author Jens Röwekamp, Tianlong Du
 *
 */
public class PrefixSpanDelta {

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
				PrefixSpanDelta prefixSpan;
				if(decoder instanceof DummyInputDecoder){
					prefixSpan = new PrefixSpanDelta(null,decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
				}else{
					prefixSpan = new PrefixSpanDelta(args[0],decoder,Double.parseDouble(args[2]),Integer.parseInt(args[3]));
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
	private DataSet<Tuple2<int[],Long>> output;
	
	/**
	 * Constructor.
	 * @param inputFile URI of the input file
	 * @param decoder to use to decode the input file
	 * @param minSupport as percentage from total number of sequences between 0 and 1
	 * @param maxPatternLength of result patterns
	 * @throws Exception
	 */
	public PrefixSpanDelta(String inputFile, InputDecoder decoder, double minSupport, int maxPatternLength) throws Exception{
		this.inputFile = inputFile;
		this.maxPatternLength = maxPatternLength;
		env = ExecutionEnvironment.getExecutionEnvironment();
		database = decoder.parse(inputFile, env);
		totalNumberOfSequences = database.count();
		System.out.println("Total number of sequences: " + totalNumberOfSequences);
		threshold = (long)Math.ceil(minSupport * totalNumberOfSequences);
		System.out.println("Threshold: " + threshold);
		//Initiate output dataset
		output = env.fromElements(new Tuple2<int[],Long>(new int[]{},0L)).filter(new AlwaysFalseFilter());
	}
	
	/**
	 * Executes the prefixSpan algorithm using deltaIterations.
	 * @throws Exception
	 */
	public void run() throws Exception{
		// find frequent base items to keep the generated patterns / search space minimal
		DataSet<Tuple1<Integer>> baseItems = database.flatMap(new BaseItemFlatMapper()).groupBy(0).sum(1).filter(new BaseItemFilter(threshold)).project(0);
		List<Tuple1<Integer>> baseItemsList = baseItems.collect();
		
		boolean resultFound = false; //Status variable if a result was found
		
		//We do it for each baseItem individually to reduce the memory usage during growth phase.
		for(Tuple1<Integer> baseItem : baseItemsList){ //TODO could be parallelized in different threads / assigned to different nodes / reads dataset in each iteration new from datasource
			// create the initial workset by expanding the database
			// <sequence, prefix, pointer, counter>
			DataSet<Tuple4<int[],int[],Integer,Long>> embeddings = database.flatMap(new SizeOneEmbeddingExpander(baseItem.f0));
		
			// create an empty initial solution set, by applying a always false filter, to instantiate the dataset.
			// <frequent pattern, frequency>
			DataSet<Tuple2<int[],Long>> allFrequentPatterns = env.fromElements(new Tuple2<int[],Long>(new int[]{},0L)).filter(new AlwaysFalseFilter());
		
			// create the initial DeltaIteration
			DeltaIteration<Tuple2<int[],Long>, Tuple4<int[], int[], Integer, Long>> whileNotEmpty =
					allFrequentPatterns.iterateDelta(embeddings, maxPatternLength, 0);
		
			// find frequent patterns of current pattern length
			DataSet<Tuple2<int[],Long>> frequentPatterns = whileNotEmpty.getWorkset()
	                .groupBy(0,1).min(2)  // distinct report per item and pattern
	                .groupBy(1).sum(3)   // count support
	                .filter(new MinSupportFilter(threshold))  // {return f.3 >= threshold}
	                .project(1,3); 		// {return f.1, f.3}
		 
			// grow embeddings of frequent patterns for next iteration
			DataSet<Tuple4<int[], int[], Integer, Long>> grownEmbeddings =
	                whileNotEmpty.getWorkset()
	                        .join(frequentPatterns)
	                        .where(1).equalTo(0)
	                        .with(new LeftSideOnly())	// only return the left side table; filter frequent ones
	                        .flatMap(new PatternGrower()); //TODO still most time intensive part --> continuous paging to hdd during first grow
		
			// add frequentPatterns to solution while grownEmbeddings not empty
			allFrequentPatterns = whileNotEmpty.closeWith(frequentPatterns, grownEmbeddings);
		
			//collect all frequent patterns
			List<Tuple2<int[],Long>> outputList = allFrequentPatterns.collect();
			//write output to result file or print it to std. out if dummy encoder was used
			 if(outputList.size() > 0){
				resultFound = true;
				output = output.union(env.fromCollection(outputList)).setParallelism(1);
			 }
		}
		if(resultFound){
			if(inputFile == null){ //dummyEncoder
				output.map(new OutputMapper()).print();
			}else{
				output.map(new OutputMapper()).writeAsCsv(inputFile+".result.csv").sortLocalOutput(1, Order.DESCENDING);
				env.execute();
				System.out.println("Frequent patterns written to: " + inputFile+".result.csv");	
			}
		}else{
			System.out.println("No frequent patterns found.");
		}
	}
	
	/**
	 * Extracts the base items of the database.
	 * No delimiting 0s are returned.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class BaseItemFlatMapper implements FlatMapFunction<Tuple2<Long,int[]>, Tuple2<Integer,Long>> {
		private static final long serialVersionUID = 8916622873868433406L;
		public void flatMap(Tuple2<Long,int[]> database, Collector<Tuple2<Integer,Long>> coll) throws Exception{
			//container for unique items
			Set<Integer> uniqueItems = new HashSet<Integer>();
			//add every item of sequence to set
			for(Integer item : database.f1){
				uniqueItems.add(item);
			}
			//construct the result
			for(Integer uniqueItem : uniqueItems){
				//no delimiting 0 shall be counted
				if(uniqueItem != 0)	coll.collect(new Tuple2<Integer,Long>(uniqueItem,1L));
			}
		}
	}
	
	/**
	 * Filters base items which occur less than threshold.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class BaseItemFilter implements FilterFunction<Tuple2<Integer,Long>>{
		private static final long serialVersionUID = 7996269067554680714L;
		private Long threshold;
		public BaseItemFilter(Long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple2<Integer, Long> baseItem) throws Exception {
			return baseItem.f1 >= threshold;
		}
	}
	
	/**
	 * Creates the initial workset by expanding the database for one baseItem.
	 * @author Jens Röwekamp, Tianlong Du
	 * <Integer[] sequence, Integer[] prefix, Integer pointer, Integer count>
	 */
	private class SizeOneEmbeddingExpander implements FlatMapFunction<Tuple2<Long,int[]>, Tuple4<int[],int[], Integer, Long>> {
		private static final long serialVersionUID = 7149640915833830565L;
		private Integer baseItem;
		public SizeOneEmbeddingExpander(Integer baseItem){
			this.baseItem = baseItem;
		}
		@Override
		public void flatMap(Tuple2<Long, int[]> database, Collector<Tuple4<int[], int[], Integer, Long>> coll) throws Exception {
			for(int pointer=0; pointer<database.f1.length; pointer++){
				if(database.f1[pointer] == baseItem)
					coll.collect(new Tuple4<int[],int[],Integer,Long>(database.f1,new int[]{database.f1[pointer]},pointer+1,1L));
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
	private class MinSupportFilter implements FilterFunction<Tuple4<int[],int[],Integer,Long>>{
		private static final long serialVersionUID = 7122121952386429180L;
		private long threshold;
		public MinSupportFilter(long threshold){
			this.threshold = threshold;
		}
		@Override
		public boolean filter(Tuple4<int[], int[], Integer, Long> sequence) throws Exception {
			return sequence.f3 >= threshold;
		}
	}
	
	/**
	 * Returns only the left side of the join. (sequence)
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class LeftSideOnly implements JoinFunction<Tuple4<int[],int[],Integer,Long>,Tuple2<int[],Long>,Tuple4<int[],int[],Integer,Long>>{
		private static final long serialVersionUID = -7336613986684255988L;
		@Override
		public Tuple4<int[], int[], Integer, Long> join(Tuple4<int[], int[], Integer, Long> sequence, Tuple2<int[], Long> frequentPattern) throws Exception {
			return sequence;
		}
	}
	
	/**
	 * Creates the new workset by expanding the previous processed workset.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class PatternGrower implements FlatMapFunction<Tuple4<int[],int[],Integer,Long>,Tuple4<int[],int[],Integer,Long>>{
		private static final long serialVersionUID = 6183515552062806750L;
		@Override
		public void flatMap(Tuple4<int[], int[], Integer, Long> quadruple,
				Collector<Tuple4<int[], int[], Integer, Long>> coll) throws Exception {
			int[] sequence = quadruple.f0;
			int[] prefix = quadruple.f1;
			Integer pointer = quadruple.f2;
			
			//Continue if the pointer points into the sequence
			if(pointer < sequence.length){
				Integer[] postfix = new Integer[sequence.length-pointer];
				for(int i=0; i<postfix.length; i++){
					postfix[i] = sequence[i+pointer];
				}
				
				//Grow the new workset
				boolean insideItemSet = true;
				for(int i=0; i<postfix.length; i++){
					if(postfix[i] == 0) insideItemSet = false;
					if(insideItemSet){
						int[] grownPrefix = new int[prefix.length+1];
						for(int j=0; j<prefix.length; j++){
							grownPrefix[j] = prefix[j];
						}
						grownPrefix[prefix.length] = postfix[i];
						coll.collect(new Tuple4<int[],int[],Integer,Long>(sequence,grownPrefix,pointer+i+1,1L));
					}else{
						if(postfix[i] != 0){
							int[] grownPrefix = new int[prefix.length+2];
							for(int j=0; j<prefix.length; j++){
								grownPrefix[j] = prefix[j];
							}
							grownPrefix[prefix.length] = 0;
							grownPrefix[prefix.length+1] = postfix[i];
							coll.collect(new Tuple4<int[],int[],Integer,Long>(sequence,grownPrefix,pointer+i+1,1L));
						}
					}
				}
			}			
		}
	}
	
	/**
	 * Transforms the dataset into output format for file output.
	 * @author Jens Röwekamp, Tianlong Du
	 *
	 */
	private class OutputMapper implements MapFunction<Tuple2<int[],Long>,Tuple2<String,Long>>{
		private static final long serialVersionUID = -7504701395976637502L;
		@Override
		public Tuple2<String, Long> map(Tuple2<int[], Long> fpattern) throws Exception {
			StringBuilder b = new StringBuilder("<");
			for(int item : fpattern.f0){
				if(item == 0) b.append("|");
				else b.append(" "+item+" ");
			}
			b.append(">");
			return new Tuple2<String,Long>(b.toString(),fpattern.f1);
		}
	}
}
