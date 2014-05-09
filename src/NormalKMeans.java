import java.io.File;
import java.util.ArrayList;
import java.util.Scanner;


public class NormalKMeans {
	public static DataPoint [] kmeanIteration(DataPoint [] data,DataPoint [] seeds)
	{
		ArrayList<ArrayList<DataPoint>> array = new ArrayList<ArrayList<DataPoint>>(seeds.length);
		for (int i = 0; i < seeds.length; i++)
			array.add(i, new ArrayList<DataPoint>());

		for (int i = 0; i < data.length; i++) {
			int index = 0;
			double min = data[i].getDistance(seeds[0]);
			for (int j = 0; j < seeds.length; j++) {
				if (seeds[j].equals(data[i]))
					continue;
				if (data[i].getDistance(seeds[j]) < min) {
					min = data[i].getDistance(seeds[j]);
					index = j;
				}
			}
			array.get(index).add(data[i]);
		}


		// String points="";
		DataPoint [] newCentroids = new DataPoint[seeds.length];
		int x=0;
		for (int i = 0; i < array.size(); i++) {
			double sepalLength = 0;
			double sepalWidth = 0;
			double petalLength = 0;
			double petalWidth = 0;
			int count = 0;
		//	String points="";
			int [] types = new int [3];
			for(int j=0;j<array.get(i).size();j++) {
				DataPoint next = array.get(i).get(j);
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
			newCentroids[x++]= new DataPoint(newCentroid);
			System.out.println("setosa:"+types[0]+"   versicolor:"+types[1]+"   virginica:"+types[2]);
	}
		
	
		
		return newCentroids;
	}
	public static void start (DataPoint [] seeds)throws Exception
 	{
		Scanner scanner = new Scanner(new File("iris.txt"));
		ArrayList<DataPoint> in = new ArrayList<DataPoint>();
		while (scanner.hasNext()) {
			String line = scanner.nextLine();
			in.add(new DataPoint(line));
		}
		scanner.close();
		DataPoint[] data = new DataPoint[in.size()];
		in.toArray(data);
		DataPoint [] before =seeds;
		boolean finished =false;
		do{

			DataPoint after[] = NormalKMeans.kmeanIteration(data, before);
			
			System.err.println("****************");
			System.err.println("Comparing old and new...");
			for(int i = 0;i<before.length;i++)
			{
				DataPoint old = before[i];
				DataPoint newPoint = after[i];
				double distance=old.getDistance(newPoint);
				System.out.println("OLD: "+old.toString()+"\n"+"NEW "+newPoint.toString());
				System.out.println(distance);
				if(Math.abs(distance)>0.05)
					break;
				if(i==before.length-1)
					finished=true;
			}
			System.err.println("****************");
			before = after;
		}while(!finished);
		
	}
	
}
