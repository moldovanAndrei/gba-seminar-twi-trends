package com.twitrends.tools;

public final class EncodingHelper {

	/**
	 * Checks if a String has valid encoding.
	 *
	 * Valid encodings includes the following Unicode characters:
	 * 		ASCII Digits
	 * 		Latin Alphabet Uppercase
	 * 		Latin Alphabet Lowercase
	 * 		Latin 1 Supplement letters
	 * 		Latin Extended-A letters
	 * 		Latin Extended-B (Slovenian + Croatian + Romanian)
	 *
	 * NOTE: Whitespaces, Math, Punctuation, Non-European, Historic Latin, Deprecated and other
	 * miscelaneous symbols are ignored.
	 *
	 * Encoding values from @link https://en.wikipedia.org/wiki/List_of_Unicode_characters
	 */
	public boolean isValidEncoded(String string) {
		boolean isCharValid;
		for (char c : string.toCharArray()) {
			isCharValid = isAsciiDigit(c) || isLatinUppercase(c) || isLatinLowercase(c) || isLatinSupplementLetter(c)
					|| isExtendedALetter(c) || isExtendedBLetter(c);
			if(!isCharValid) {
				return false;
			}
		}
		return true;
	}

	/**
	 * ASCII Digits 			(0 -> 9): 	#048 -> #057
	 */
	private boolean isAsciiDigit(char c) {
		if (c >= 48 && c <= 57) {
			return true;
		}
		return false;
	}

	/**
	 * Latin Alphabet Uppercase (A -> Z):	#065 -> #090
	 */
	private boolean isLatinUppercase(char c) {
		if (c >= 65 && c <= 90) {
			return true;
		}
		return false;
	}

	/**
	 * Latin Alphabet Lowercase	(a -> z):	#097 ->	#122
	 */
	private boolean isLatinLowercase(char c) {
		if (c >= 97 && c <= 122) {
			return true;
		}
		return false;
	}

	/**
	 * Latin 1 Supplement letters 			#192 ->	#214
	 * #216 ->	#246
	 * #248 ->	#255
	 */
	private boolean isLatinSupplementLetter(char c) {
		if (c >= 192 && c <= 214) {
			return true;
		}
		if (c >= 216 && c <= 246) {
			return true;
		}
		if (c >= 248 && c <= 255) {
			return true;
		}
		return false;
	}

	/**
	 * Latin Extended-A letters				#256 -> #383
	 */
	private boolean isExtendedALetter(char c) {
		if (c >= 256 && c <= 383) {
			return true;
		}
		return false;
	}

	/**
	 * Latin Extended-B
	 * Slovenian + Croatian + Romanian	#512 -> #539
	 */
	private boolean isExtendedBLetter(char c) {
		if (c >= 512 && c <= 539) {
			return true;
		}
		return false;
	}

}
