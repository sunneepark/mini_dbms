package com.dp.miniDBMS;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.security.SecureRandom;

public class MakeSql {
	private static SecureRandom random = new SecureRandom();

	/** 랜덤 문자열을 생성한다 **/
	public static String generate(String DATA, int length) {
		if (length < 1)
			throw new IllegalArgumentException("length must be a positive number.");
		StringBuilder sb = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			sb.append(DATA.charAt(random.nextInt(DATA.length())));
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		String ENGLISH_LOWER = "abcdefghijklmnopqrstuvwxyz";
		String ENGLISH_UPPER = ENGLISH_LOWER.toUpperCase();
		String NUMBER = "0123456789";

		/** 랜덤을 생성할 대상 문자열 **/
		String DATA_FOR_RANDOM_STRING = ENGLISH_LOWER + ENGLISH_UPPER + NUMBER;

		/** 랜덤 문자열 길이 **/
		int random_string_length = 10;
		try {
			File file = new File("./data1.txt");
			// 입력 스트림 생성
			FileWriter filereader = new FileWriter(file);
			// 입력 버퍼 생성
			BufferedWriter bufReader = new BufferedWriter(filereader);

			for (int i = 0; i < 150000; i++) {
				double dValue = Math.random();

				int c1 = (int) (dValue * 70000) + 1;
				double dValue2 = Math.random();
				int c2 = (int) (dValue2 * 10000) + 1;
				String s = "insert into sunny.snoopy values (" + c1 + "," + c2 + ",\'"
						+ generate(DATA_FOR_RANDOM_STRING, random_string_length) + "\');\n";

				bufReader.write(s);
			}
			bufReader.close();
		} catch (FileNotFoundException e) {
			System.out.println(e);
		} catch (IOException e) {
			System.out.println(e);
		}
	}
}
