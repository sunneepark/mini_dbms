package com.dp.miniDBMS.sm.smo;

import com.dp.miniDBMS.sm.smu.GlobalConst;

/**
  *
  * @날짜 : Aug 21, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : 페이지를 나타내는 클래스 
  *
 */
public class Page implements GlobalConst {
	
	public int pid; // page id
	protected byte[] data; //page 실제 데이터 

	// --------------------------------------------------------------------------

	/**
	 * Default constructor; creates a blank page.
	 */
	public Page() {
		data = new byte[PAGE_SIZE];
	}
	public Page(int pid, byte[] data) {
		this.pid = pid;
		this.data = data;
	}

	/**
	 * Constructor that wraps the given byte array.
	 */
	public Page(byte[] data) {
		setData(data);
	}

	/**
	 * Get accessor for the data byte array.
	 */
	public byte[] getData() {
		return data;
	}

	/**
	 * Set accessor for the data byte array.
	 * 
	 * @throws IllegalArgumentException if the data array size is invalid
	 */
	public void setData(byte[] data) {
		if (data.length != PAGE_SIZE) {
			new IllegalArgumentException("Invalid page buffer size");
		}
		this.data = data;
	}

	/**
	 * Sets this page's data array to share the given page's data array.
	 */
	public void setPage(Page page) {
		this.data = page.data;
	}

	/**
	 * Copies the contents of the given page's buffer into this page's buffer.
	 */
	public void copyPage(Page page) {
		System.arraycopy(page.data, 0, this.data, 0, PAGE_SIZE);
	}

	// --------------------------------------------------------------------------

	/**
	 * Gets an int at the given page offset.
	 */
	public int getIntValue(int offset) {
		return Convert.getIntValue(offset, data);
	}

	/**
	 * Sets an int at the given page offset.
	 */
	public void setIntValue(int value, int offset) {
		Convert.setIntValue(value, offset, data);
	}

	/**
	 * Gets a string at the given page offset, given the maximum length.
	 */
	public String getStringValue(int offset, int length) {
		return Convert.getStringValue(offset, data, length);
	}

	/**
	 * Sets a string at the given page offset.
	 */
	public void setStringValue(String value, int offset, int length) {
		Convert.setStringValue(value, offset, data, length);
	}
}
