# Facebook data collector

Based on Deen's work (https://github.com/dfreelon/fb_scrape_public).
This script can download posts and comments from public Facebook pages and groups (__but not users__). It requires Python 3.

Instructions
------------

1.    This script is written for Python 3 and won't work with previous Python versions.
2.    You need to create your own Facebook app, which you can do here: https://developers.facebook.com/apps . Doesn't matter what you call it, you just need to pull the unique client ID (app ID) and app secret for your new app.
3.    Once you create your app, you can insert the client ID and secret AS STRINGS into the appropriate fields. 
4.    This function accepts text FB user IDs ('barackobama'), numerical user IDs, and post IDs (but these must contain the page or group ID as a prefix; see [here](https://stackoverflow.com/questions/31353591/how-should-we-retrieve-an-individual-post-now-that-post-id-is-deprecated-in-v)). 

Sample code
-----------

```python
from fb_data_collector import FacebookAuthenticator
from fb_data_collector import FacebookPostsCollector
from fb_data_collector import FacebookCommentsCollector

#below, client_id and client_secret should be your actual client ID and secret
app_id = "<>"
client_secret = "<>"

fb_auth = FacebookAuthenticator(app_id,client_secret)
fb_access_token = fb_auth.request_access_token()

#to get page posts
posts_collector = FacebookPostsCollector(fb_access_token)
posts = posts_collector.collect("barackobama",max_rows=100)

#to get comments on a single post
comments_collector = FacebookCommentsCollector(fb_access_token)
post_id = "6815841748_10155375836346749"

comments = comments_collector.collect(post_id,max_rows=100)

```
