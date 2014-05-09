import java.io.File;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class KPlusPlus {
	public void nearest(DataPoint pt, DataPoint center, int cluster) {
	}

	public int findCeil(double arr[], double r, int l, int h) {
		int mid;
		while (l < h) {
			mid = l + ((h - l) >> 1);
			if (r > arr[mid])
				l = mid + 1;
			else
				h = mid;
		}
		return (arr[l] >= r) ? l : -1;
	}

	public int myRand(DataPoint arr[], double freq[], int n) {
		double prefix[] = new double[n];
		prefix[0]=freq[0];
		for (int i = 1; i < n; ++i)
			prefix[i] = prefix[i - 1] + freq[i];
		Random generator = new Random();
		double random = generator.nextInt((int) prefix[n - 1]) + 1
				+ generator.nextDouble();
		int indexc = findCeil(prefix, random, 0, n - 1);
		return indexc;

	}
	public double [] calculateDistances(DataPoint [] data,int seed)
	{
		double [] distances = new double [data.length];
		int i=0;
		for(DataPoint x:data)
			distances[i++]=data[seed].getDistance(x);
		return distances;
	}
	public DataPoint [] kmeansPlusPlus(int K,DataPoint  data[])
	{
		ArrayList<Integer> chosen = new ArrayList<Integer>();
		DataPoint seeds [] = new DataPoint [K];
		Random generator = new Random();
		int random =  generator.nextInt(K);
		double distances [] = calculateDistances(data,random);
		for(int i=0;i<K;i++)
		{
			int pos=myRand(data,distances,data.length);
			seeds[i]=data[pos];
			chosen.add(i);
			random =  pos;
			distances = calculateDistances(data,random);
			for(int x:chosen)
				distances[x]=0;
		
		}
		return seeds; 
	}

	public static void main(String[] args) throws Exception {
		
		
	}

}
