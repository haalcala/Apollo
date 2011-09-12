package org.apollo.orm;

public class CassanateException extends Exception {
	private static final long serialVersionUID = -5743751828263668114L;

	public CassanateException() {
		super();
	}

	public CassanateException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public CassanateException(String message) {
		super(message);
	}

	public CassanateException(Throwable cause) {
		super(cause);
	}
}
