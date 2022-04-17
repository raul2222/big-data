from pygooglenews import GoogleNews
# default GoogleNews instance
gn = GoogleNews(lang = 'en', country = 'US')

top = gn.top_news(proxies=None, scraping_bee = None)
print(top)