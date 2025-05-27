"""
Utility module for generating IDs.
"""

import random

# List of words that can be used for IDs
# Words must be shorter than 6 characters to leave room for numbers
WORDS = [
    "sun",
    "cat",
    "dog",
    "fox",
    "owl",
    "bee",
    "sky",
    "sea",
    "air",
    "ice",
    "oak",
    "elm",
    "ant",
    "bat",
    "pig",
]


def generate_id() -> str:
    """Generate an ID with a word followed by numbers, always 6 characters total.

    Returns:
        A string like 'sun123' or 'cat001' or 'owl420'
    """
    word = random.choice(WORDS)
    remaining_digits = 6 - len(word)
    number = random.randint(0, 10**remaining_digits - 1)
    return f"{word}{str(number).zfill(remaining_digits)}"
