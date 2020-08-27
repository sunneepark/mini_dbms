package com.dp.miniDBMS.sm.smo;

/**
  *
  * @날짜 : Aug 20, 2020 
  * @작성자 : 박선희 
  * @클래스설명 : int 와 string byte 로 변환 
  *
 */
public class Convert {

	/**
	 * Reads from the given byte array at the specified position, and converts it
	 * into an integer.
	 */
	public static int getIntValue(int pos, byte[] data) {

		return ((((int)data[pos] & 0xff) << 24) |
				(((int)data[pos+1] & 0xff) << 16) |
				(((int)data[pos+2] & 0xff) << 8) |
				(((int)data[pos+3] & 0xff)));
	} 
	
	/**
	 * Writes an integer into the given byte array at the specified position.
	 */
	public static void setIntValue(int value, int pos, byte[] data) {
		data[pos] = (byte) (value >> 24);
		data[pos + 1] = (byte) (value >> 16);
		data[pos + 2] = (byte) (value >> 8);
		data[pos + 3] = (byte) (value);
	}
	/**
	 * Reads from the given byte array at the specified position, and converts it to
	 * a string of given length.
	 */
	public static String getStringValue(int pos, byte[] data, int length) {

		int buflen = data.length - pos;
		if (buflen < length) {
			length = buflen;
		}

		return new String(data, pos, length).trim();

	} 

	/**
	 * Writes a string into the given byte array at the specified position.
	 */
	public static void setStringValue(String value, int pos, byte[] data, int length) {
		byte[] ba = value.getBytes();
		System.arraycopy(ba, 0, data, pos, ba.length);
		for(int i = ba.length ; i < length ; i++) {
			data[pos+i] = (byte)0;
		}
	} 

}
