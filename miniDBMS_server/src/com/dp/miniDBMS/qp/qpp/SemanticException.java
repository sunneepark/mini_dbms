package com.dp.miniDBMS.qp.qpp;

public class SemanticException extends Exception {

	public SemanticException(String message) {
		super("[semantic error]" + message);
	}
}
