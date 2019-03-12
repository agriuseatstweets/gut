# gut
Processes and analyses tweets

## structure 
'main.py' contains all parameters, 'MAIN.sh' is the main script. 

## descriptives
descriptives have the following properties: 
- followers_count: 
    - user.followers_count: denotes the number of followers of user.screen_name at time created_at
    - user.friends_count: denotes the number of friends of user.screen_name at time created_at, that is the number of users the user is following
- engagement_counts: 
  - original_tweet_count: the number of original tweets (non-retweets, non-replies, non-quote-tweets) user.screen_name has created that are in the dataset
  - retweet_count: the number of retweets of tweets created by user.screen_name in the dataset
  - reply_count: the number of replies to tweets created by user.screen_name in the dataset
  - mention_count: the number of mentions of user.screen_name in the dataset
- edges_weights: 
    - the number of users that performed the respective engagement with user1 and user2 in the dataset