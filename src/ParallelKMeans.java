import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSOutputSummer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ParallelKMeans {
	public static class DataMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<DataPoint> centroid = new ArrayList<DataPoint>();
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
					.getConfiguration());
			FileInputStream fs = new FileInputStream(new File(
					cacheFiles[0].toString()));

			DataInputStream in = new DataInputStream(fs);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String l;

			while ((l = br.readLine()) != null)
				centroid.add(new DataPoint(l));

			br.close();
			in.close();
			fs.close();

			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				DataPoint current = new DataPoint(st.nextToken());
				double min = current.getDistance(centroid.get(0));
				DataPoint closestCentroid = centroid.get(0);
				for (DataPoint x : centroid) {
					if (x.equals(current))
						continue;
					if (current.getDistance(x) < min) {
						min = current.getDistance(x);
						closestCentroid = x;
					}
				}
				context.write(new Text(closestCentroid.toString()), new Text(
						current.toString()));
				System.out.println(closestCentroid.toString() + "  | "
						+ current.toString());
			}
		}

	}

	public static class DataCombiner extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Iterator<Text> val = values.iterator();
			while (val.hasNext())
				context.write(key, val.next());
		}
	}

	public static class DataReducer extends Reducer<Text, Text, Text, Text> {

		FileSystem fs;
		String seedsName = "/user/hduser/seeds.txt";
		Path outFile = new Path(seedsName);
		FSDataOutputStream fsdos;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			fs = FileSystem.get(context.getConfiguration());
			fsdos = fs.create(outFile);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Iterator<Text> val = values.iterator();
			double sepalLength = 0;
			double sepalWidth = 0;
			double petalLength = 0;
			double petalWidth = 0;
			int count = 0;
		//	String points="";
			int [] types = new int [3];
			while (val.hasNext()) {
				DataPoint next = new DataPoint(val.next().toString());
				sepalLength += next.getSepalLength();
				sepalWidth += next.getSepalWidth();
				petalLength += next.getPetalLength();
				petalWidth += next.getPetalWidth();
				count++;
				//points+=next.getType()+"$$";
				if(next.getType().equals("Iris-setosa"))
					types[0]++;
				else if(next.getType().equals("Iris-versicolor"))
					types[1]++;
				else if (next.getType().equals("Iris-virginica"))
					types[2]++;
			}

			sepalLength = sepalLength / count;
			sepalWidth = sepalWidth / count;
			petalLength = petalLength / count;
			petalWidth = petalWidth / count;

			String newCentroid = sepalLength + "," + sepalWidth + ","
					+ petalLength + "," + petalWidth + "," + "null";
			fsdos.writeBytes(newCentroid + "\n");
			//context.write(new Text(newCentroid), new Text(points));
			System.out.println("setosa:"+types[0]+"   versicolor:"+types[1]+"   virginica:"+types[2]);
			//System.out.println(points);
			
		}

		/**
		 * Called once at the end of the task.
		 */
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// NOTHING
			fsdos.close();

		}

	}

	public static void main(String[] args) throws Exception {
		/*
		 * Initialization steps get the seeds for k means
		 */
		Scanner scanner = new Scanner(new File("iris.txt"));
		ArrayList<DataPoint> in = new ArrayList<DataPoint>();
		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			in.add(new DataPoint(line));
		}
		scanner.close();
		DataPoint[] data = new DataPoint[in.size()];
		in.toArray(data);
		DataPoint[] seeds = new KPlusPlus().kmeansPlusPlus(6, data);
		long time = System.currentTimeMillis();
		NormalKMeans.start(seeds);
		System.out.println("Time : "+(System.currentTimeMillis()-time));
		int counter=0;
		/*
		 * Write the seeds into the file...
		 */
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Invalid number of input must have 2 input");
			System.exit(2);
		}
		FileSystem fs = FileSystem.get(conf);
		String seedsName = "/user/hduser/seeds.txt";
		Path outFile = new Path(seedsName);
		FSDataOutputStream fsdos = fs.create(outFile);
		for (DataPoint x : seeds) {
			fsdos.writeBytes(x.toString() + "\n");
			System.out.println(x.toString());
		}
		fsdos.close();
		boolean finished = false;
		time = System.currentTimeMillis();
		do {
			DistributedCache.addCacheFile(new URI(seedsName), conf);
			DistributedCache.addCacheFile(new URI(args[0]), conf);
		
			FSDataInputStream input = fs.open(new Path(seedsName));
			String strLine;
			DataPoint [] before =  new DataPoint[seeds.length];
			int x=0;
			while ((strLine = input.readLine()) != null) 
				before[x++]= new DataPoint(strLine);
			input.close();
			
			
			
			Job job = new Job(conf, "kmeans");
			job.setJarByClass(ParallelKMeans.class);
			job.setInputFormatClass(NLinesInputFormat.class);
			job.setMapperClass(DataMapper.class);
			job.setCombinerClass(DataCombiner.class);
			job.setReducerClass(DataReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"_"+(counter++)));
			job.waitForCompletion(true);
			
			input = fs.open(new Path(seedsName));
			DataPoint [] after =  new DataPoint[seeds.length];
			 x=0;
			while ((strLine = input.readLine()) != null) 
				after[x++]= new DataPoint(strLine);
			input.close();
			if(before.length!=after.length)
				System.out.println("@@@ CENTROIDS SIZES ARE NOT THE SAME @@@");
			System.err.println("****************");
			System.err.println("Comparing old and new...");
			for(int i = 0;i<before.length;i++)
			{
				DataPoint old = before[i];
				DataPoint newPoint = after[i];
				double distance=old.getDistance(newPoint);
				System.out.println("OLD: "+old.toString()+"\n   "+"NEW "+newPoint.toString());
				System.out.println(distance);
				if(Math.abs(distance)>0.05)
					break;
				if(i==before.length-1)
					finished=true;
			}
			System.err.println("****************");
			
			
		} while (!finished);
		System.out.println("Total time : "+(System.currentTimeMillis()-time)/(1000*60));

	}
}
