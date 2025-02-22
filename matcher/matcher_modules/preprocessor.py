import regex as re
import pandas as pd
from unidecode import unidecode
from tqdm import tqdm

tqdm.pandas()

# Compile regex patterns once
# Compile regex patterns once for efficiency
SUFFIXES_RE = re.compile(r'\b(inc|corp|ltd|llc|company|co|sas)\b', re.IGNORECASE)
SPECIAL_CHARS_RE = re.compile(r'[\p{P}\p{S}]', re.IGNORECASE)
MULTIPLE_SPACES_RE = re.compile(r'\s+')
NON_LATIN_RE = re.compile(r'[^\p{Latin}\s\d]', re.IGNORECASE)
NUMBERS_RE = re.compile(r'\d+')

def preprocess_name(name):
    """
    Preprocesses a company name by cleaning, transliterating, and extracting numbers.

    Parameters:
        name (str): The original company name.

    Returns:
        tuple: A tuple containing:
            - processed_name (str or None): The cleaned and transliterated name.
            - is_transliterated (bool): Flag indicating if the name was transliterated.
            - numbers (list): List of numbers extracted from the name.
    """
    if isinstance(name, float) or pd.isnull(name):  # Handle NaN or missing values
        return None, False, []

    # Convert to lowercase
    name = name.lower()

    # Extract numbers from the name
    numbers = NUMBERS_RE.findall(name)

    # Remove common company suffixes
    name_cleaned = SUFFIXES_RE.sub('', name)

    # Remove punctuation and symbols
    name_cleaned = SPECIAL_CHARS_RE.sub('', name_cleaned)

    # Normalize whitespace
    name_cleaned = MULTIPLE_SPACES_RE.sub(' ', name_cleaned).strip()

    # If the name is empty after cleaning, return None
    if not name_cleaned:
        return None, False, numbers

    # Check for non-Latin characters
    if NON_LATIN_RE.search(name_cleaned):
        # Transliterate non-Latin characters to Latin alphabet
        name_transliterated = unidecode(name_cleaned)
        return name_transliterated, True, numbers  # Return transliterated name, flag, and numbers
    else:
        return name_cleaned, False, numbers  # Return cleaned name, flag, and numbers

def preprocess_dataframe(df, name_column):
    """Preprocess a complete dataframe"""
    df[['processed_name', 'is_transliterated', 'numbers']] = df[name_column].progress_apply(
    lambda x: pd.Series(preprocess_name(x)))
    df = df.dropna(subset=['processed_name'])
    return df