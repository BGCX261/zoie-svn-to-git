package proj.zoie.api;


public class ZoieException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ZoieException() {
		super();
	}

	public ZoieException(String message, Throwable cause) {
		super(message, cause);
	}

	public ZoieException(String message) {
		super(message);
	}

	public ZoieException(Throwable cause) {
		super(cause);
	}
}
