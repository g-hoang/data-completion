import re
import textdistance


def string_similarity(string1, string2):
    """Calculate similarity of two strings"""
    # Remove brackets
    if type(string1) is not str or type(string2) is not str:
        return 0

    string1 = re.sub(r'\(\d*\)', '', string1).lower()
    string1 = re.sub(r'\,', '', string1)
    string1 = re.sub(r'\.', '', string1)
    string1 = re.sub(r'\t', ' ', string1)
    string1 = re.sub(r'\n', ' ', string1).strip()

    string2 = re.sub(r'\(\d*\)', '', string2).lower()
    string2 = re.sub(r'\,', '', string2)
    string2 = re.sub(r'\.', '', string2)
    string2 = re.sub(r'\t', ' ', string2)
    string2 = re.sub(r'\n', ' ', string2).strip()

    edit_sim = textdistance.levenshtein.normalized_similarity(string1, string2)
    jacc_sim = textdistance.jaccard.normalized_similarity(string1, string2)

    return 0.2 * edit_sim + 0.8 * jacc_sim


def string_containment(string1, string2):
    """Check if one string token set is contained in the other"""
    if type(string1) is not str or type(string2) is not str:
        return False

    string1 = re.sub(r'\(\d*\)', '', string1).lower()
    string1 = re.sub(r'\,', '', string1)
    string1 = re.sub(r'\.', '', string1)
    string1 = re.sub(r'\t', ' ', string1)
    string1 = re.sub(r'\n', ' ', string1).strip()
    string1_tokens = string1.split(' ')

    string2 = re.sub(r'\(\d*\)', '', string2).lower()
    string2 = re.sub(r'\,', '', string2)
    string2 = re.sub(r'\.', '', string2)
    string2 = re.sub(r'\t', ' ', string2)
    string2 = re.sub(r'\n', ' ', string2).strip()
    string2_tokens = string2.split(' ')

    return set(string1_tokens).issubset(set(string2_tokens)) or set(string2_tokens).issubset(set(string1_tokens))
