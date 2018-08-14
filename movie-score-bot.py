# Reddit Movie Score bot created by Evan Cole, August 2018
# this bot parses the Official Discussion threads for new films on /r/movies and posts critic/audience scores and does some simple analysis determing if critics and audiences agree 
# could possibly extend to get scores for a specific movie if a user "pages" the bot and requests it?

import cinemascore
import datetime
from imdb import IMDb, IMDbError
import ssl
from bs4 import BeautifulSoup
import requests
import re
import praw
import time
import os
import psycopg2
import psycopg2.extras

# ==========================================================
# SCORE FUNCTIONS - HOW THE DATA IS COLLECTED

# wrapper function for collecting data
def collectData(title,critics,audiences,i,rt_url,user_agent):
    movie, metacritic = getIMDBinfo(title, i)

    audiences['IMDB Users'] = imdb_score(i,movie)
    audiences['Cinemascore'] = get_cinemascore(title)

    if metacritic: 
        meta_url = getMetacriticURL(metacritic['metacritic url'])
        critics['Metacritic'] = metacritic['metascore']
        audiences['Metacritic Users'] = metacritic_user_score(meta_url,user_agent)

    if rt_url: 
        rottenTomatoes(critics,audiences,rt_url,user_agent)

    # reddit poll temporarily disabled - have to figure out how to scrape the mean
    #if poll_url:
        #audiences['Reddit Poll'] = reddit_poll_score(poll_url,user_agents)
        #print(title, audiences['Reddit Poll'])
        #print(title,audiences['Reddit Poll'])
    
# returns a Movie object and metacritic data dictionary in order to efficiently request film data using IMDBPy
# because IMDBPy can get us both IMDB and Metacritic information, this function exists so we don't have to repeat method calls in both Metacritic and IMDB functions
def getIMDBinfo(filmTitle, i):
    matches = i.search_movie(filmTitle) 
    movie = matches[0] 
    ID = movie.movieID

    metacritic_data = i.get_movie_critic_reviews(ID)['data']
    return movie, metacritic_data

# returns weighted average IMDB user rating
def imdb_score(i, movie):
    i.update(movie,'vote details')
    imdb = movie['demographics']['imdb users']['rating']
    return imdb

# RottenTomatoes wrapper
# calls all the RT functions and puts any found scores into the necessary dicts
def rottenTomatoes(critics,audiences,url,user_agent):
    request = requests.get(url,headers={'User-Agent':user_agent})
    page = request.content
    soup = BeautifulSoup(page, 'html.parser')

    critics['Rotten Tomatoes']['all_score'], critics['Rotten Tomatoes']['all_avg'] = rt_critic_scores(soup,'all')
    if critics['Rotten Tomatoes']['all_score'] is not None and critics['Rotten Tomatoes']['all_avg'] is not None: # if there's no general critic rating, there's nothing else either
        critics['Rotten Tomatoes']['top_score'], critics['Rotten Tomatoes']['top_avg'] = rt_critic_scores(soup,'top')
        audiences['Rotten Tomatoes Audience']['aud_score'], audiences['Rotten Tomatoes Audience']['aud_avg'] = rt_audience_score(soup)
    
# returns RottenTomatoes critic scores if exists, otherwise None
# parameters are BeautifulSoup object and a mode string, which determines whether it's looking for the scores for all critics, or top ones
# mode should always be 'all' or 'top'
def rt_critic_scores(soup,mode):
    # percent rating
    scoreStr = 'div[id=' + mode +'-critics-numbers] span.meter-value.superPageFontColor span'
    score_query = soup.select_one(scoreStr)
    if not score_query: # element not found - abort
        return None, None
    score = score_query.text

    # average rating
    avgStr = 'div[id=' + mode +'-critics-numbers] div.superPageFontColor'
    avg_query = soup.select_one(avgStr)
    if not avg_query:
        return None, None
    avg = avg_query.contents[-1].strip().split('/')[0] # score is between two closing tags, this is how we get it - also removes the /10 part of the scoree

    return score, avg

# returns RottenTomatoes audience scores  
def rt_audience_score(soup):
    # percent rating
    span = soup.find('span',{'class':'superPageFontColor', 'style':'vertical-align:top'}) # I don't think BeautifulSoup's select function accepts style tags
    if not span:
        return None, None
    score = span.text[:-1] # strip off the %

    # average rating
    avgStr = 'div.audience-info.hidden-xs.superPageFontColor div'
    avg_query = soup.select_one(avgStr)
    if not avg_query:
        return None, None
    avg = avg_query.contents[-1].strip().split('/')[0] # double closing tag funkiness + trimming

    return score, avg

# returns metacritic user score
def metacritic_user_score(url,user_agent):
    request = requests.get(url+'/user-reviews',headers={'User-Agent':user_agent})
    page = request.content
    soup = BeautifulSoup(page, 'html.parser')

    # user rating
    scoreStr = 'td.num_wrapper > span'
    score_query = soup.select_one(scoreStr)
    if not score_query:
        return None
    score = score_query.text
    if score == 'tbd':
        return None
    return score

# returns reddit poll score - not using this right now
def reddit_poll_score(url,user_agent):
    request = requests.get(url+'/user-reviews',headers={'User-Agent':user_agent})
    page = request.content
    soup = BeautifulSoup(page, 'html.parser')

    #print(soup.prettify())

    scoreStr = 'span.rating-mean-value'
    score_query = soup.select_one(scoreStr)
    if not score_query:
        return None
    score = score_query.text
    return score

# returns a film's cinemascore if exists, otherwise None
def get_cinemascore(filmTitle):
    # keys in the results dictionary are named as "FILM TITLE (YEAR)"
    # get current year to conform to this convention
    filmTitle = filmTitle.upper()

    # films that start with "The" are listed on Cinemascore in "___, The" form, so split the title by whitespace and check (if so, change the title string)
    split_title = filmTitle.split()
    if len(split_title) > 1 and split_title[0] == 'THE':
        filmTitle = ' '.join(split_title[1:]) + ', THE'

    now = datetime.datetime.now()
    key = filmTitle + ' (' + str(now.year) + ')'
    
    search = cinemascore.search(filmTitle)
    if key in search:
        return search[key]
    else: # film not on CinemaScore
        return None

# ==========================================================
# URL FUNCTIONS - KNOWING WHERE TO GET THE DATA

# parses the discussion post for necessary URLs (Rotten Tomatoes, the r/movies poll)
# returns URL if exists, None otherwise
# parameter is a text block split by newline (for line-by-line reading) and a mode string - "rt" for Rotten Tomatoes, "poll" for Reddit poll, and "meta" for Metacritic
def parseThreadForURL(block,mode):
    url = None
    for line in block: # go through text block line by line looking for url
        r = None
        if mode == 'rt': # rotten tomatoes
            r = re.match(r"\*\*Rotten\s+Tomatoes:\*\*\s+\[[0-9]{1,3}(?:%|&#37;)\]\((https://www.rottentomatoes.com/m/.*)\)\s*", line)
        elif mode == 'poll': # reddit poll
            r = re.match(r".*\((https://youpoll.me/[0-9]*/r)\)s*",line)
        elif mode == 'meta': # can also get Metacritic URL via this function, but IMDBPy seems safer (this function is dependent on parsing a post created by a human and prone to failing if any typos or formatting changes occur)
            r = re.match(r"\*\*Metacritic:\*\*\s+\[[0-9]{1,3}/100\]\((http://www.metacritic.com/movie/.*)\)\s*",line)
        if r: # match found
            url = r.groups()[0]
            break      
    return url

# returns and processes Metacritic URL 
def getMetacriticURL(url):
    queryIndex = url.find('?') # cut out the query part of the link (it isn't necessary)
    url = url[:queryIndex]
    return url

# ==========================================================
# COMMENT FUNCTIONS - PROCESSING/CREATING COMMENT STRING

# wrapper - processes collected data in order to create the comment that the bot will post
def createComment(title, critics, scales, audiences):
    now = datetime.datetime.now()
    currentTime = now.strftime("%m/%d/%Y, %I:%M %p EST")
    comment = 'Critic/Audience scores for *' + title + '* as of ' + currentTime + ':\n\n' + criticBlock(critics,scales) + '\n' + audienceBlock(audiences,scales) + '\n' + analysis(critics,audiences)
    return comment

# createComment helper function for critic scores
def criticBlock(critics,scales):
    block = '**Critics**  \n\n'
    for source in critics:
        if source == 'Rotten Tomatoes':
            data = critics[source]['all_score']
        else:
            data = critics[source]
        block += ('* ' + source + ': ')
        if data is not None: # data exists
            block += (str(data) + scales[source])
            if source == 'Rotten Tomatoes':
                block += (' liked it, average rating ' + str(critics[source]['all_avg']) + '/10')
                if critics[source]['top_score'] is not None and critics[source]['top_avg'] is not None:
                    block += (' (Top Critics: ' + str(critics[source]['top_score']) + '%, average rating ' + str(critics[source]['top_avg']) + '/10)')
        else:
            block += 'Not Available'
        block += '  \n\n'
    return block

# createComment helper function for audience scores
def audienceBlock(audiences,scales):
    block = '**Audiences**  \n\n'
    for source in audiences:
        if source == 'Rotten Tomatoes Audience':
            data = audiences[source]['aud_score']
        else:
            data = audiences[source]
        block += ('* ' + source + ': ')
        if data is not None:
            block += str(data)
            if source != 'Cinemascore': # Cinemascore is only non-numerical rating, so no scales for that
                block += scales[source]
                if source == 'Rotten Tomatoes Audience':
                    block += (' liked it, average rating ' + str(audiences[source]['aud_avg']) + '/5')
        else:
            block += 'Not Available'
        block += '  \n\n'
    return block

# ==========================================================
# ANALYSIS FUNCTIONS - GETTING AVERAGES AND DETERMINING WHETHER CRITICS AND AUDIENCES AGREE

def analysis(critics,audiences):
    block = ''
    critic_avg = averageScore(critics,'critic')
    aud_avg = averageScore(audiences,'aud')
    if critic_avg is not None and aud_avg is not None:
        block += 'The average critic score is ' + str(critic_avg) + '/10. The average audience score is ' + str(aud_avg) + '/10. '

        in_the_ballpark = False
        if (critic_avg < 6 and aud_avg < 6) or (critic_avg >= 6 and aud_avg >= 6):
            in_the_ballpark = True
        
        if in_the_ballpark:
            thresholds = [1,1.5,2.2]
        else:
            thresholds = [1,1.3,2]
        
        difference = abs(critic_avg - aud_avg)
        if difference <= thresholds[0]:
            block += ' Critics and audiences agree.'
            # critics and audiences agree
        elif difference <= thresholds[1]:
            block += ' Critics and audiences slightly disagree.'
            # somewhat disagreement
        elif difference <= thresholds[2]:
            block += 'Critics and audiences somewhat disagree.'
        else:
            block += 'Critics and audiences definitely disagree.'

    return block
        
# gets the averages of averages, essentially - goes through a score dictionary and averages the scores there
# parameters are a score dictionary (critics or audiences), and the mode - 'critics' and 'aud'
def averageScore(scoreDict,mode):
    if mode == 'aud':
        # maps Cinemascores to a numerical value - uses Metacritic's scoring guide
        cinemascore_key = {'A+':10,'A':10,'A-':9.1,'B+':8.3,'B':7.5,'B-':6.7,'C+':5.8,'C':5,'C-':4.2,'D+':3.3,'D':2.5,'D-':1.6,'F+':0.8,'F':0,'F-':0} 
        rt_prefix = 'aud'
    else:
        rt_prefix = 'all'

    total = 0
    n = 0
    for source in scoreDict:
        if 'Rotten Tomatoes' in source: # handling the weirdness of the nested RT dicts (since it carries multiple scores)
            score = scoreDict[source][rt_prefix+'_avg'] 
        else:
            score = scoreDict[source]

        if score is not None: # if score exists, add it to the average total
            if source == 'Cinemascore':
                score = cinemascore_key[score]
            else:
                score = float(score)
                # convert everything to 10-point system
                if source == 'Metacritic': # /100 -> /10
                    score = score / 10 
                elif source == 'Rotten Tomatoes Audience': # /5 -> /10
                    score = (score/5)*10
            total += score
            n += 1
    
    if n < 1: # no scores available - can't get an average
        avg = None
    else:
        avg = round(total / n,1)
    return avg

# ==========================================================
# DRIVER FUNCTIONS - where most of the heavy lifting occurs (didn't want main() to get too long)
def processBot(reddit, db_cursor, user_agent, start_time): 
    try:
        i = IMDb()
    except IMDbError as e:
        print('ERROR: Something went wrong with IMDBPy -',e)
        return

    total = 0
    for submission in reddit.subreddit("movies").search('official discussion',sort='new',time_filter='week'):
        # avoid post duplication
        db_cursor.execute("""SELECT exists(SELECT 1 FROM submissions WHERE submissionID=(%s)) as exists""",(submission.id,))
        alreadyPosted = db_cursor.fetchone()['exists']
        if alreadyPosted:
            print('Already posted in thread',submission.id,'-',submission.title)
        else:
            r = re.match(r"Official Discussion(?::\s+|\s+-\s+)([^(]*).*[\[\\(\{]SPOILERS[\]\\)\}].*",submission.title) # ugly regex, but works (only have to shave off whitespace at the end of the title)
            if r:
                title = r.groups()[0].strip()

                critics = {'Metacritic': None,'Rotten Tomatoes': {'all_score': None, 'all_avg': None, 'top_score':None, 'top_avg':None}} # critic scores
                scales = {'Metacritic':'/100','Rotten Tomatoes':'%','Metacritic Users':'/10','Rotten Tomatoes Audience':'%','IMDB Users':'/10','Reddit Poll':'/10'} # point systems for relevant data sources
                audiences = {'Metacritic Users': None,'Rotten Tomatoes Audience': {'aud_score':None, 'aud_avg':None}, 'IMDB Users': None,'Cinemascore': None} # audience scores

                post = submission.selftext.split('---') # split post into sections for efficient parsing 
                #poll_block = post[0].split('\n')  # Poll section of post - used for getting Reddit poll URL
                score_block = post[-1].split('\n') # Critic score section of post - used for getting critic site URLs

                rt_url = parseThreadForURL(score_block,'rt') # RT url
                #poll_url = parseThreadForURL(poll_block,'poll') # Reddit poll url

                collectData(title,critics,audiences,i,rt_url,user_agent)

                comment = createComment(title,critics,scales,audiences)
                print('=============================')
                print(comment)

                submission.reply(comment)
                db_cursor.execute("""INSERT INTO submissions (submissionID) VALUES (%s)""",(submission.id,))

                total += 1

    elapsed = time.time() - start_time
    print()
    print(total, 'posts in',time.strftime("%H:%M:%S", time.gmtime(elapsed)))
 
def main():
    start = time.time()
    ssl._create_default_https_context = ssl._create_unverified_context # monkey patch for getting past SSL errors (this might just be a problem for my Mac)
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14'
    
    reddit = praw.Reddit(client_id=os.environ['CLIENT_ID'],
                         client_secret=os.environ['CLIENT_SECRET'],
                         username=os.environ['REDDIT_USERNAME'],
                         password=os.environ['REDDIT_PASSWORD'],
                         user_agent='Movie Score Bot 1.0')

    try:
        conn = psycopg2.connect(os.environ['DATABASE_URL'],sslmode='require')
    except psycopg2.Error as e:
        print("ERROR: Can't connect to the database -",e)
        return

    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    processBot(reddit,cur,user_agent,start)

main()