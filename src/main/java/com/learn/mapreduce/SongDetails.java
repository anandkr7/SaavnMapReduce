package com.learn.mapreduce;

public class SongDetails {

	private String songId;
	private String date;
	private String hour;
	private int played;
	private float zScore;

	public String getSongId() {
		return songId;
	}

	public void setSongId(String songId) {
		this.songId = songId;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getHour() {
		return hour;
	}

	public void setHour(String hour) {
		this.hour = hour;
	}

	public int getPlayed() {
		return played;
	}

	public void setPlayed(int played) {
		this.played = played;
	}

	@Override
	public String toString() {
		return "SongDetails [songId=" + songId + ", date=" + date + ", hour=" + hour + ", played=" + played + "]";
	}

	public float getzScore() {
		return zScore;
	}

	public void setzScore(float zScore) {
		this.zScore = zScore;
	}

}
