package com.upgrad.mapreduce.domain;

public class Song {

	private String songId;
	private String date;
	private String hour;
	private int played;
	private double zScore;

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

	public double getzScore() {
		return zScore;
	}

	public void setzScore(double zScore) {
		this.zScore = zScore;
	}

}
