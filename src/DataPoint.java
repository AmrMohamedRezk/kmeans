import java.util.StringTokenizer;

public class DataPoint implements Comparable<DataPoint>{
	private double sepalLength;
	private double sepalWidth;
	private double petalLength;
	private double petalWidth;
	private String type;

	public DataPoint(String data) {
		StringTokenizer st = new StringTokenizer(data, ",");
		this.sepalLength = Double.parseDouble(st.nextToken());
		this.sepalWidth = Double.parseDouble(st.nextToken());
		this.petalLength = Double.parseDouble(st.nextToken());
		this.petalWidth = Double.parseDouble(st.nextToken());
		this.type = st.nextToken();

	}

	public double getSepalLength() {
		return sepalLength;
	}

	public void setSepalLength(double sepalLength) {
		this.sepalLength = sepalLength;
	}

	public double getSepalWidth() {
		return sepalWidth;
	}

	public void setSepalWidth(double sepalWidth) {
		this.sepalWidth = sepalWidth;
	}

	public double getPetalLength() {
		return petalLength;
	}

	public void setPetalLength(double petalLength) {
		this.petalLength = petalLength;
	}

	public double getPetalWidth() {
		return petalWidth;
	}

	public void setPetalWidth(double petalWidth) {
		this.petalWidth = petalWidth;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public boolean equals(Object o) {
		DataPoint other = (DataPoint) o;
		return (this.petalLength == other.petalLength
				&& this.petalWidth == other.petalWidth
				&& this.sepalLength == other.sepalLength
				&& this.sepalWidth == other.sepalWidth && this.type == other.type);
	}
	
	public double getDistance(DataPoint other)
	{
		if(this.equals(other))
			return 0;
		double deltaPL = Math.pow((this.petalLength-other.petalLength), 2);
		double deltaPW = Math.pow((this.petalWidth-other.petalWidth), 2);
		double deltaSL = Math.pow((this.sepalLength-other.sepalLength), 2);
		double deltaSW = Math.pow((this.sepalWidth-other.sepalWidth), 2);
		double sum = deltaPL+deltaPW+deltaSL+deltaSW;
		return Math.sqrt(sum);
	}

	@Override
	public int compareTo(DataPoint other) {
		if(this.equals(other))
		return 0;
		double dist = this.getDistance(other);
		return (dist > 0) ? 1:-1;
	}
	public String toString()
	{
		return sepalLength+","+sepalWidth+","+petalLength+","+petalWidth+","+type;
	}
}
