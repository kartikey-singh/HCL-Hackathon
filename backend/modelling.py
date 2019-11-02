from nltk.stem import WordNetLemmatizer, SnowballStemmer
from sklearn.feature_extraction.text import CountVectorizer
from datetime import datetime, date, time, timedelta
from textblob import TextBlob
from operator import itemgetter
import pandas as pd
import tweepy as tw
import gensim
import nltk
import re
import os


ACCESS_TOKEN = "306251107-7jJxsp07G2loVykD2b0yIWQz4iZigWSlT2lsrftV"
ACCESS_SECRET = "6L0uAbBp2ARvtIfEzg3easmIru0K6oJjXjRPJ6D1LR9RB"
CONSUMER_KEY = "JEjqNkTTod6LSygPPgERi1SFs"
CONSUMER_SECRET = "0876hK6ApXg0hbDSjTJAgJH7V5fXxhuyPgfmvDWGbw5yOLjIWd"

CITIES = {
    'DELHI': '28.609692,77.205833,40km',
    'LAS_VEGAS': '36.135912,-115.150135,30km',
    'LUCKNOW': '26.862802,80.935388,30km',
    'VARANASI': '25.322125,82.973680,30km'
}

TOPICS = 5
MAX_TWEETS = 50


def get_tweets(city, max_tweets):
    stemmer = SnowballStemmer("english")
    auth = tw.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
    api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    tweets = tw.Cursor(api.search, lang='en', geocode=city).items(max_tweets)
    # users_data = [[tweet.user.screen_name, tweet.user.location, tweet.user.followers_count,
    #                tweet.user.friends_count, tweet.text] for tweet in tweets]

    users_data = [[tweet.user.screen_name, tweet.user.location, tweet.user.followers_count, tweet.user.friends_count, tweet.text, tweet.retweet_count, tweet.favorite_count, tweet.user.listed_count, tweet.user.statuses_count, len(
        tweet.entities["user_mentions"]), int(tweet.in_reply_to_status_id_str != None), int(tweet.user.verified), len(tweet.entities["hashtags"]), (datetime.utcnow()-tweet.user.created_at).days, int(tweet.retweet_count != 0)] for tweet in tweets]

    return users_data


def create_dataframe(users_data):
    # df = pd.DataFrame(data=users_data, columns=[
    #                   'user', 'location', 'followers', 'following', 'unfiltered_text'])
    
    df = pd.DataFrame(data=users_data, columns=['user', 'location', 'followers', 'following', 'unfiltered_text', 'retweets',
                                                'likes', 'listed_count', 'tweets', 'mentions', 'isreply', 'verified', 'hashtags', 'days', 'retweeted_before'])    
    return df


def lemmatize_stemming(text):
    return WordNetLemmatizer().lemmatize(text, pos='v')
    # return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))


def tokenize_and_lemmatize(text):
    text = re.sub(r"http\S+", "", text)
    text = text.lower()
    tokens = [word for sent in nltk.sent_tokenize(
        text) for word in nltk.word_tokenize(sent)]
    filtered_tokens = []
    punctuations = "[^a-zA-Z0-9]"

    for token in tokens:
        if len(token) > 3:
            filtered_tokens.append(lemmatize_stemming(
                re.sub(punctuations, '', token)))

    st = ' '.join(filtered_tokens)
    return ' '.join(st.split())


def topic_modelling(df):
    vect = CountVectorizer(stop_words='english',
                           token_pattern='(?u)\\b\\w\\w\\w+\\b')
    # Fit and transform
    X = vect.fit_transform(df['text'])
    # Convert sparse matrix to gensim corpus.
    corpus = gensim.matutils.Sparse2Corpus(X, documents_columns=False)
    # Mapping from word IDs to words (To be used in LdaModel's id2word parameter)
    id_map = dict((v, k) for k, v in vect.vocabulary_.items())

    ldamodel = gensim.models.ldamodel.LdaModel(
        corpus=corpus, passes=25, random_state=34, num_topics=TOPICS, id2word=id_map)

    return ldamodel, vect


def lda_topics(ldamodel):
    ans = [(i, ldamodel.print_topic(topicno=i)) for i in range(TOPICS)]
    return ans


def topic_distribution(row, ldamodel, vect):
    row = [row]
    X = vect.transform(row)
    corp = gensim.matutils.Sparse2Corpus(X, documents_columns=False)
    topic_list = list(ldamodel.get_document_topics(corp))[0]
    return max(topic_list, key=itemgetter(1))[0]


def sentiment_analysis(row):
    # create TextBlob object of passed tweet text
    analysis = TextBlob(row)
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'


def calculate_popularity(df):
    df3 = pd.DataFrame()

    def normalize(df):
        result = df.copy()
        result = df / max(df)
        #result = df / np.linalg.norm(df)
        return result

    df3['followers'] = normalize(df['followers'])*.1677
    df3['listed_count'] = normalize(df['listed_count'])*.1583
    df3['fol/tweet'] = normalize(df['followers']/df['tweets'])*.1392
    df3['listed/tweet'] = normalize(df['listed_count']/df['tweets'])*.1239
    df3['mentions'] = normalize(df['mentions'])*.0521
    df3['isreply'] = normalize(df['isreply'])*.0431
    df3['verified'] = normalize(df['verified'])*.0363
    df3['hashtags'] = normalize(df['hashtags'])*.0224
    df3['days'] = normalize(df['days'])*.0206
    df3['retweeted_before'] = normalize(df['retweeted_before'])*.0206
    df3['ig'] = df3['followers'] + df3['listed_count']+df3['fol/tweet']+df3['listed/tweet'] + \
        df3['mentions']+df3['isreply']+df3['verified'] + \
        df3['hashtags']+df3['days']+df3['retweeted_before']
    igmean = df3['ig'].mean()
    df3['popularity'] = (df3['ig']/igmean)**.5
    df['popularity'] = df3['popularity']
    return df


def process():
    for city, location in CITIES.items():
        users_data = get_tweets(location, MAX_TWEETS)
        print('got data')
        df = create_dataframe(users_data)
        df['text'] = df['unfiltered_text'].apply(tokenize_and_lemmatize)
        # df['popularity'] = df['followers']/df['following']
        df = calculate_popularity(df)
        df['city'] = city
        ldamodel, vect = topic_modelling(df)
        df['topics'] = df['text'].apply(
            topic_distribution, args=(ldamodel, vect))
        df['sentiment'] = df['text'].apply(sentiment_analysis)
        df = df.sort_values(['topics', 'popularity'], ascending=[True, False])

        df = df.sort_values('popularity', ascending=False).groupby(
            'topics').head(10)
        df = df.sort_values(['topics', 'popularity'], ascending=[True, False])

        df.to_csv('backend/Cities/' + city + '.csv')


if __name__ == "__main__":
    # for city, location in CITIES.items():
    #     users_data = get_tweets(location, MAX_TWEETS)
    #     print('got data')
    #     df = create_dataframe(users_data)
    #     df['text'] = df['unfiltered_text'].apply(tokenize_and_lemmatize)
    #     df['popularity'] = df['followers']/df['following']
    #     df['city'] = city
    #     ldamodel, vect = topic_modelling(df)
    #     df['topics'] = df['text'].apply(
    #         topic_distribution, args=(ldamodel, vect))
    #     df = df.sort_values(['topics', 'popularity'], ascending=[True, False])
    #     df.to_csv('backend/Cities/' + city + '.csv')
    process()
