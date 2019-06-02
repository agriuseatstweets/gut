# gut
Processes and analyses tweets

## Preprocessing and Loading

Tweets are read from json files in GCS and processed in the following manner (preprocessing.loading):

1. "Limit tweets" are removed.

2. The full text of the tweet is collected from either the extended_tweet attribute or the retweeted_status (or it's extended_tweet attribute). The same is done for the entities (user mentions being the entities of interest).

3. Tweets are trimmed to contain only relevant attributes.

4. Creation times are localized to IST (+5:30) and all tweets outside the specified window (On or after April 11th 00:00 and before May 25th 00:00 IST) are filtered out.

5. Tweets are filtered out which do not fit one of the following criteria:

* It includes one of the keywords of interest in the full text (step 2) or quoted text.
* It includes one of the candidates or parties (twitter usernames or other alternative names) in the full text (step 2) or quoted text.
* It is posted by one of the candidates or parties.
* It is a retweet of one of the candidates or parties.
* It is a quote tweet that is quoting one of the candidates or parties.
* It is posted as a reply to one of the candidates or parties.

6. Duplicates are removed.
