package com.upgrad.mapreduce.domain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SongTextWritable implements Writable, WritableComparable<SongTextWritable> {

	private Text songId;
	private Text date;

	@Override
	public int compareTo(SongTextWritable obj) {
		int result = this.date.compareTo(obj.date);
		if (result == 0) {
			result = this.songId.compareTo(obj.songId);
		}
		return result;
	}

	public SongTextWritable(Text songId, Text date) {
		setSongId(songId);
		setDate(date);
	}

	public SongTextWritable() {
		setSongId(new Text());
		setDate(new Text());
	}

	public SongTextWritable(String songId, String date) {
		setSongId(new Text(songId));
		setDate(new Text(date));
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		songId.readFields(input);
		date.readFields(input);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		songId.write(output);
		date.write(output);
	}

	public Text getSongId() {
		return songId;
	}

	public void setSongId(Text songId) {
		this.songId = songId;
	}

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result + ((songId == null) ? 0 : songId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SongTextWritable other = (SongTextWritable) obj;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (songId == null) {
			if (other.songId != null)
				return false;
		} else if (!songId.equals(other.songId))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return songId + "," + date;
	}

}
