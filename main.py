from pyspark import SparkConf, SparkContext
import numpy as np
import json
import datetime
import re

most_common_words = ["the", "of", "to", "and", "a", "in", "is", "it", "you", "that", "he", "was", "for", "on", "are",
                     "with", "as", "I", "his", "they", "be", "at", "one", "have", "this", "from", "or", "had", "by",
                     "hot", "but", "some", "what", "there", "we", "can", "out", "other", "were", "all", "your", "when",
                     "up", "use", "word", "how", "said", "an", "each", "she", "which", "do", "their", "time", "if",
                     "will", "way", "about", "many", "then", "them", "would", "write", "like", "so", "these", "her",
                     "long", "make", "thing", "see", "him", "two", "has", "look", "more", "day", "could", "go", "come",
                     "did", "my", "sound", "no", "most", "number", "who", "over", "know", "water", "than", "call",
                     "first", "people", "may", "down", "side", "been", "now", "find", "any", "new", "work", "part",
                     "take", "get", "place", "made", "live", "where", "after", "back", "little", "only", "round", "man",
                     "year", "came", "show", "every", "good", "me", "give", "our", "under", "name", "very", "through",
                     "just", "form", "much", "great", "think", "say", "help", "low", "line", "before", "turn", "cause",
                     "same", "mean", "differ", "move", "right", "boy", "old", "too", "does", "tell", "sentence", "set",
                     "three", "want", "air", "well", "also", "play", "small", "end", "put", "home", "read", "hand",
                     "port", "large", "spell", "add", "even", "land", "here", "must", "big", "high", "such", "follow",
                     "act", "why", "ask", "men", "change", "went", "light", "kind", "off", "need", "house", "picture",
                     "try", "us", "again", "animal", "point", "mother", "world", "near", "build", "self", "earth",
                     "father", "head", "stand", "own", "page", "should", "country", "found", "answer", "school", "grow",
                     "study", "still", "learn", "plant", "cover", "food", "sun", "four", "thought", "let", "keep",
                     "eye", "never", "last", "door", "between", "city", "tree", "cross", "since", "hard", "start",
                     "might", "story", "saw", "far", "sea", "draw", "left", "late", "run", "don't", "while", "press",
                     "close", "night", "real", "life", "few", "stop", "open", "seem", "together", "next", "white",
                     "children", "begin", "got", "walk", "example", "ease", "paper", "often", "always", "music",
                     "those", "both", "mark", "book", "letter", "until", "mile", "river", "car", "feet", "care",
                     "second", "group", "carry", "took", "rain", "eat", "room", "friend", "began", "idea", "fish",
                     "mountain", "north", "once", "base", "hear", "horse", "cut", "sure", "watch", "color", "face",
                     "wood", "main", "enough", "plain", "girl", "usual", "young", "ready", "above", "ever", "red",
                     "list", "though", "feel", "talk", "bird", "soon", "body", "dog", "family", "direct", "pose",
                     "leave", "song", "measure", "state", "product", "black", "short", "numeral", "class", "wind",
                     "question", "happen", "complete", "ship", "area", "half", "rock", "order", "fire", "south",
                     "problem", "piece", "told", "knew", "pass", "farm", "top", "whole", "king", "size", "heard",
                     "best", "hour", "better", "TRUE", "during", "hundred", "am", "remember", "step", "early", "hold",
                     "west", "ground", "interest", "reach", "fast", "five", "sing", "listen", "six", "table", "travel",
                     "less", "morning", "ten", "simple", "several", "vowel", "toward", "war", "lay", "against",
                     "pattern", "slow", "center", "love", "person", "money", "serve", "appear", "road", "map",
                     "science", "rule", "govern", "pull", "cold", "notice", "voice", "fall", "power", "town", "fine",
                     "certain", "fly", "unit", "lead", "cry", "dark", "machine", "note", "wait", "plan", "figure",
                     "star", "box", "noun", "field", "rest", "correct", "able", "pound", "done", "beauty", "drive",
                     "stood", "contain", "front", "teach", "week", "final", "gave", "green", "oh", "quick", "develop",
                     "sleep", "warm", "free", "minute", "strong", "special", "mind", "behind", "clear", "tail",
                     "produce", "fact", "street", "inch", "lot", "nothing", "course", "stay", "wheel", "full", "force",
                     "blue", "object", "decide", "surface", "deep", "moon", "island", "foot", "yet", "busy", "test",
                     "record", "boat", "common", "gold", "possible", "plane", "age", "dry", "wonder", "laugh",
                     "thousand", "ago", "ran", "check", "game", "shape", "yes", "hot", "miss", "brought", "heat",
                     "snow", "bed", "bring", "sit", "perhaps", "fill", "east", "weight", "language", "among"]


def score_to_score_bucket(score):
    if score < 10:
        return "under_10"
    elif score < 25:
        return "10_25"
    elif score < 50:
        return "25_50"
    elif score < 100:
        return "50_100"
    elif score < 250:
        return "100_250"
    elif score < 500:
        return "250_500"
    elif score < 1000:
        return "500_1000"
    elif score < 2500:
        return "1000_2500"
    elif score < 5000:
        return "2500_5000"

    return "over_5000"


def map_to_score_and_save_as_file(rdd, file_name):
    rdd.groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile(file_name)


def story_analysis(sc):
    myRDD = sc.textFile("small_data_random.txt").map(lambda line: json.loads(line))

    myRDD.filter(lambda entry: entry['text'] not in ["", None]) \
        .map(lambda entry: (
        [x for x in re.sub('(\W|\d)', ' ', entry['text']).lower().split()], entry['score'])) \
        .flatMap(lambda data: [(trigram, data[1]) for trigram in zip(data[0], data[0][1:], data[0][2:])]) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: (result[1][4], result[1][0]), False) \
        .saveAsTextFile("content/trigrams/scores_by_text_trigrams")

    myRDD.map(lambda entry: (
        [x for x in re.sub('(\W|\d)', ' ', entry['title']).lower().split()], entry['score'])) \
        .flatMap(lambda data: [(trigram, data[1]) for trigram in zip(data[0], data[0][1:], data[0][2:])]) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: (result[1][4], result[1][0]), False) \
        .saveAsTextFile("content/trigrams/scores_by_title_trigrams")

    map_to_score_and_save_as_file(
        myRDD.filter(lambda entry: entry['text'] not in ["", None])
             .map(lambda entry: ([x for x in set(re.sub('(\W|\d)', ' ', entry['text']).lower().split()) if
                                 (x not in most_common_words) is True], entry['score']))
            .flatMap(lambda data: [(word, data[1]) for word in data[0]])
        , "content/word/scores_by_word_text")

    myRDD.map(lambda entry: (score_to_score_bucket(entry['score']), entry['descendants'])) \
        .groupByKey() \
        .mapValues(
        lambda comments: [np.mean(list(comments)), np.var(list(comments)), np.max(list(comments)),
                          np.min(list(comments)),
                          len(list(comments))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("story/comments_by_score_bucket")

    myRDD.map(lambda entry: (
        [x for x in set(re.sub('(\W|\d)', ' ', entry['title']).lower().split()) if
         (x not in most_common_words) == True],
        entry['score'])) \
        .flatMap(lambda data: [(word, data[1]) for word in data[0]]) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("content/word/scores_by_word_title")

    myRDD.filter(lambda entry: entry['url'] is not None) \
        .filter(lambda entry: entry['url'].startswith('http')) \
        .map(
        lambda entry: (re.compile("^(?:https?:\/\/)?(?:www\.)?([^:\/\n?]+)").findall(entry['url'])[0], entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("story/scores_by_domain")

    myRDD.map(lambda entry: (entry['author'], entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("story/scores_by_author")

    myRDD.map(lambda entry: (
        (datetime.datetime.fromtimestamp(entry['time']).year, datetime.datetime.fromtimestamp(entry['time']).month),
        entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("time/scores_by_year_month")

    myRDD.map(lambda entry: (datetime.datetime.fromtimestamp(entry['time']).year, entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("time/scores_by_year")

    myRDD.map(lambda entry: (datetime.datetime.fromtimestamp(entry['time']).month, entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("time/scores_by_month")

    myRDD.map(lambda entry: (datetime.datetime.fromtimestamp(entry['time']).day, entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("time/scores_by_day")

    myRDD.map(lambda entry: (datetime.datetime.fromtimestamp(entry['time']).hour, entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("time/scores_by_hour")


def story_comments_analysis(sc):
    myRDD = sc.textFile("small_data_random_s_c.txt").map(lambda line: json.loads(line))

    myRDD.map(lambda entry: (entry['author_1'], entry['score'])) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("comments/scores_by_author")

    myRDD.filter(lambda entry: entry['text_1'] not in ["", None]) \
        .map(lambda entry: (
        [x for x in set(re.sub('(\W|\d)', ' ', entry['text_1']).lower().split()) if
         (x not in most_common_words) == True],
        entry['score'])) \
        .flatMap(lambda data: [(word, data[1]) for word in data[0]]) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: result[1][0], False) \
        .saveAsTextFile("content/word/scores_by_word_comment")

    myRDD.filter(lambda entry: entry['text_1'] not in ["", None]) \
        .map(lambda entry: (
        [x for x in re.sub('(\W|\d)', ' ', entry['text_1']).lower().split()], entry['score'])) \
        .flatMap(lambda data: [(trigram, data[1]) for trigram in zip(data[0], data[0][1:], data[0][2:])]) \
        .groupByKey() \
        .mapValues(
        lambda scores: [np.mean(list(scores)), np.var(list(scores)), np.max(list(scores)), np.min(list(scores)),
                        len(list(scores))]) \
        .sortBy(lambda result: (result[1][4], result[1][0]), False) \
        .saveAsTextFile("content/trigrams/scores_by_comment_trigrams")


def month_analysis(month, year):
    myRDD = sc.textFile("small_data_random_s_c.txt").map(lambda line: json.loads(line))
    file_prefix = "month_analysis/year/month/"

    # top author
    # top commentor
    # top domains
    # title, text, comment trigrams
    # scores by day


if __name__ == '__main__':
    conf = SparkConf().setMaster("local").setAppName("My App")
    sc = SparkContext(conf=conf)

    story_comments_analysis(sc)
    story_analysis(sc)
