package utility.delay;

/**
 * Enum for string parsing error fixing
 */
public enum DelayFixes {
	gen("1"),
	feb("2"),
	mar("3"),
	apr("4"),
	mag("5"),
	giu("6"),
	lug("7"),
	ago("8"),
	set("9"),
	ott("10"),
	nov("11"),
	dic("12");

	private String text;

	DelayFixes(String text) {
		this.text = text;
	}

	public String getText() {
		return this.text;
	}


}
