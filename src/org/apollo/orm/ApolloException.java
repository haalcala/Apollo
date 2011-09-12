package org.apollo.orm;

public class ApolloException extends Exception {
	private static final long serialVersionUID = -5743751828263668114L;

	public ApolloException() {
		super();
	}

	public ApolloException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public ApolloException(String message) {
		super(message);
	}

	public ApolloException(Throwable cause) {
		super(cause);
	}
}
