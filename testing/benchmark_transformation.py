from pyspark.sql.functions import \
    udf, col, regexp_replace, split, array, \
    when, slice, to_date, arrays_zip, sort_array, \
    explode, collect_list, count, size, create_map, \
    lit, array_contains, coalesce, size, when, \
    concat, length
from pyspark.sql.types import ArrayType, StringType, IntegerType, MapType, DoubleType, DataType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
from itertools import chain


def benchmark_body_pipeline(cleaned_dataframe,  stopwordlist=None):
    """NLP pipeline. Tokenizes, removes stopwords, and computes TF-IDF
    Returns transformed data as 'features' and the vocabulary of words."""

    tokenizer = Tokenizer(inputCol="Text", outputCol="Text_tokens")
    if stopwordlist:
        stop_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="Text_tokens_stopped",
                                        stopWords=stopwordlist)
    else:
        stop_remover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="Text_tokens_stopped")

    count_vect = CountVectorizer(inputCol=stop_remover.getOutputCol(), outputCol="Text_counts_raw")
    idf = IDF(inputCol=count_vect.getOutputCol(), outputCol="features")

    pipeline = Pipeline(stages=[tokenizer, stop_remover, count_vect, idf])
    model = pipeline.fit(cleaned_dataframe)
    featurized_data = model.transform(cleaned_dataframe)

    return featurized_data, model.stages[-2].vocabulary


def benchmark_extract_top_keywords(posts, n_keywords=10):
    """Given TF-IDF output (as "features" column) extracts out the vocabulary index of the
    10 keywords with highest TF-IDF (for each post)."""

    def extract_keys_from_vector(vector):
        return vector.indices.tolist()

    def extract_values_from_vector(vector):
        return vector.values.tolist()

    extract_keys_from_vector_udf = udf(lambda vector: extract_keys_from_vector(vector), ArrayType(IntegerType()))
    extract_values_from_vector_udf = udf(lambda vector: extract_values_from_vector(vector), ArrayType(DoubleType()))

    posts = posts.withColumn("extracted_keys", extract_keys_from_vector_udf("features"))
    posts = posts.withColumn("extracted_values", extract_values_from_vector_udf("features"))

    posts = posts.withColumn("zipped_truncated",
                             slice(sort_array(arrays_zip("extracted_values", "extracted_keys"), asc=False), 1,
                                   n_keywords))

    take_second = udf(lambda rows: [row[1] for row in rows], ArrayType(IntegerType()))
    posts = posts.withColumn("top_indices", take_second("zipped_truncated"))

    return posts


def benchmark_explode_group_filter(keyword_posts, vocab, n_words=50, vocab_lookup=True):
    """Takes in a dataframe with a top_indices column given indices of keywords for each post,
     along with a column CreationDate of dates for the respective posts.
    Output effectively inverts this key:value pair to produce a dataframe with one column
    keyword_index consisting of keywords which appeared in some top_indices row, and a column
    collect_list(CreationDate) consisting of dates on which a post was made with that keyword.

    n_words parameter sets cutoff for number of keywords to consider (chosen by maximal number of posts).
    vocab_lookup attaches a new column 'keyword_literal' giving the actual keyword (not the index).
    """

    exploded = keyword_posts.withColumn('keyword_index', explode('top_indices'))
    unexploded = exploded.groupby("keyword_index").agg(
        collect_list("CreationDate"))

    unexploded = unexploded.withColumn('n_posts', size(col("collect_list(CreationDate)")))
    unexploded = unexploded.sort('n_posts', ascending=False).limit(n_words)

    if vocab_lookup:
        max_index = unexploded.agg({"keyword_index": "max"}).collect()[0][0]
        small_vocab = vocab[:max_index + 1]  # vocabulary is sorted by total word count. Only lookup possible words.

        vocab_dict = {k: v for k, v in enumerate(small_vocab)}
        vocab_mapping = create_map([lit(x) for x in chain(*vocab_dict.items())])
        unexploded = unexploded.withColumn("keyword_literal", vocab_mapping.getItem(col("keyword_index")))

    unexploded = unexploded.where(length('keyword_literal') > 1)  # Remove hard to interpret single-character keywords
    return unexploded

def benchmark_process_all(spark, which_tag, post_link, tags_link, custom_stopwords=True):
    processed_tags = convert_tags(spark, tags_link)
    top_tags = extract_top_tags(processed_tags)
    tag = top_tags[which_tag]

    if custom_stopwords:
        stop_words = generate_stopwords(processed_tags)
        global stop_words
    else:
        pass

    cleaned_posts = convert_posts(spark, post_link)
    tag_transferred = tag_transfer(cleaned_posts)
    tag_selected = select_with_tag(tag_transferred, tag)

    if custom_stopwords:
        output_posts, vocabulary = benchmark_body_pipeline(
            tag_selected, stopwordlist=stop_words)
    else:
        output_posts, vocabulary = benchmark_body_pipeline(tag_selected)

    keyworded_posts = benchmark_extract_top_keywords(output_posts)
    final = benchmark_explode_group_filter(keyworded_posts, vocabulary)
    return final

