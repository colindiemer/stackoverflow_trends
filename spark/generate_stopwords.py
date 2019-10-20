from nltk.stem import WordNetLemmatizer
from process_tags import extract_top_tags


def generate_stopwords(tags,
                       stopwords_file="./stopwords/stopwords_10000.txt",
                       common_tech_words_file='./stopwords/common_tech_words.txt',
                       tag_min=500):
    """Generates a list of stopwords. By default takes in the file consisting of 10,000 most commmon words.
    To avoid throwing out too much, we repopulate with tech words coming from popular tags.
    Unforunately, SO has some very broad tags like "file", "web" or "function", which we then re-remove."""
    stopwords = []
    try:
        with open(stopwords_file, "r") as f:
            for line in f:
                stopwords.append(line)
        stopwords = [word.strip() for word in stopwords]
    except FileNotFoundError:
        return None

    tagwords = [tag.lower() for tag in extract_top_tags(tags, top=tag_min)]
    tagwords = [tag.split('-') for tag in tagwords]
    tagwords = [item for sublist in tagwords for item in sublist]  # flatten list
    lemmatizer = WordNetLemmatizer()
    tagwords = [lemmatizer.lemmatize(tag) for tag in tagwords]

    too_common_tag_words = []
    try:
        with open(common_tech_words_file, "r") as f:
            for line in f:
                too_common_tag_words.append(line)
        too_common_tag_words = [word.strip() for word in too_common_tag_words]
    except FileNotFoundError:
        return None

    return list(set(stopwords).difference(tagwords)) + too_common_tag_words