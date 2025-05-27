"""
Utility module for generating human-readable IDs.
"""

import random

# List of simple words that can be used for IDs
# These words are chosen to be short, clear, and easy to read/remember
ADJECTIVES = [
    "red", "blue", "green", "bold", "calm", "dark", "deep", "fast", 
    "glad", "good", "kind", "light", "loud", "mild", "neat", "new",
    "pure", "quick", "real", "soft", "warm", "wise", "brave", "cool",
    "free", "gold", "happy", "keen", "lucky", "nice", "proud", "safe",
    "sharp", "smart", "strong", "sweet", "true", "wild", "young"
]

NOUNS = [
    "air", "bird", "book", "cloud", "door", "fire", "fish", "flag",
    "game", "home", "king", "leaf", "moon", "note", "park", "path",
    "rain", "road", "rock", "sand", "star", "sun", "time", "tree",
    "wave", "wind", "wolf", "bear", "cat", "deer", "dove", "duck",
    "fox", "hawk", "lion", "owl", "rose", "sage", "sea", "sky"
]

def generate_id() -> str:
    """Generate a human-readable random ID using an adjective and a noun.
    
    Returns:
        A string like 'bold-hawk' or 'quick-fox'
    """
    adjective = random.choice(ADJECTIVES)
    noun = random.choice(NOUNS)
    return f"{adjective}-{noun}" 