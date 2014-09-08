package cc.twittertools.query;

public class Tweet {
	private long id; // tweet id
	private int rank; // rank by query likelihood score
	private long epoch; // timestamp
	private long timeDiff; // difference with query time
	private double qlScore; // query likelihood score 
	private double tmScore; // temporal model score
	private String text; // tweet contents
	
	public Tweet(long id, long epoch, long timeDiff, double qlscore) {
		this.id = id;
		this.epoch = epoch;
		this.timeDiff = timeDiff;
		this.qlScore = qlscore;
	}
	
	public Tweet(long id, int rank, long epoch, long timeDiff, double qlScore) {
		this.id = id;
		this.rank = rank;
		this.epoch = epoch;
		this.timeDiff = timeDiff;
		this.qlScore = qlScore;
	}
	
	@Override
	public boolean equals(Object other) {
		if (other == null) return false;
    if (other == this) return true;
    if (!(other instanceof Tweet))return false;
    Tweet tweet = (Tweet) other;
    return id == tweet.id;
	}
	
	public long getId() {
		return id;
	}

	public int getRank() {
		return rank;
	}

	public long getEpoch() {
		return epoch;
	}

	public long getTimeDiff() {
		return timeDiff;
	}

	public double getQlScore() {
		return qlScore;
	}
	
	public double getTMScore() {
		return tmScore;
	}
	
	public String getText() {
		return text;
	}
	
	public void setId(long id) {
		this.id = id;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	public void setEpoch(long epoch) {
		this.epoch = epoch;
	}

	public void setTimeDiff(long timeDiff) {
		this.timeDiff = timeDiff;
	}

	public void setQlScore(double qlScore) {
		this.qlScore = qlScore;
	}
	
	public void setTMScore(double tmScore) {
		this.tmScore = tmScore;
	}
	
	public void setText(String text) {
		this.text = text;
	}
	
}
