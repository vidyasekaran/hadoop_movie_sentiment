package com.dataflair.bd.poc.vo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class UserActivityVO implements Writable {

	private String movieId;
	private String title;
	private String genre;
	private int rating;
	private int userId;
	private int count;
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(movieId);
		out.writeUTF(title);
		out.writeUTF(genre);
		out.writeInt(rating);
		out.writeInt(userId);
		out.writeInt(count);

		/*
		 * out.writeUTF(sex); out.writeInt(age); out.writeUTF(occupation);
		 * out.writeUTF(zipCode);
		 */
	}

	public int getUserId() {
		return userId;
	}

	public void setUserId(int userId) {
		this.userId = userId;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		if(null!=in) {
		movieId = in.readUTF();
		title = in.readUTF();
		genre = in.readUTF();
		rating = in.readInt();
		userId = in.readInt();
		count= in.readInt();
		
		
		}
		/*
		 * sex = in.readUTF(); age = in.readInt(); occupation = in.readUTF();
		 * zipCode=in.readUTF();
		 */

	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public String getMovieId() {
		return movieId;
	}

	public void setMovieId(String movieId) {
		this.movieId = movieId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getGenre() {
		return genre;
	}

	public void setGenre(String genre) {
		this.genre = genre;
	}

	public int getRating() {
		return rating;
	}

	public void setRating(int rating) {
		this.rating = rating;
	}



	@Override
	public String toString() {
		return "UserActivityVO [movieId=" + movieId + ", title=" + title + ", genre=" + genre + ", rating=" + rating
				+ ", userId=" + userId + ", count=" + count + "]";
	}
}
