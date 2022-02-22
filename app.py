import string
from tokenize import String
import uuid
import argparse
from newsapi import NewsApiClient
from newsapi import newsapi_exception
from clickhouse_driver import Client,errors
import config
# ----------------------------------------------------------------------------------------------------------
# Prerequistes -
#
# 1. NEWS API ACcvount -
#    https://newsapi.org/account
#
# 2. ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP). -
#    https://clickhouse.com/docs/en/
# ----------------------------------------------------------------------------------------------------------
parser = argparse.ArgumentParser(description='Process connection parameters')
parser.add_argument('-k','--key', help='input newsAPIKey', required=True)
parser.add_argument('-d','--db', help='Clickhouse url', required=True)
parser.add_argument('-p','--password', help='Clickhouse password',required=True)
args = vars(parser.parse_args())
KEY_WORD = config.settings['key_word']
def get_sources_from_api():
    try:
        return newsapi.get_sources()['sources']
    except newsapi_exception.NewsAPIException as e:
          print('News API connection error:\'{0}\''.format(e.exception['message']))
          return []

def get_articles_with_sources(articles):
     # prepartion for export articles to clickhouse
    articles_with_sources =[]
    article_for_export={}
    for i in range(len(articles)):
        if(articles[i]['source']['id'] is not None):
            article_for_export = articles[i]
            article_for_export['id'] = articles[i]['source']['id']
            article_for_export["guid"] =str(uuid.uuid1())
            del article_for_export['source'] 
            articles_with_sources.append(article_for_export)
    return articles_with_sources

def load_all_articles():
    # fetching all articles by keyword
    temp_articles_storage = []
    total_results = 0
    number_of_articles = 0
    try:
        latest_articles =newsapi.get_everything(q='\'{0}\''.format(KEY_WORD),
                                       page_size=100,
                                       sort_by='publishedAt'
                                     )
    except newsapi_exception.NewsAPIException as e:
          print('News API connection error:\'{0}\''.format(e.exception['message']))
          return temp_articles_storage
    total_results =latest_articles['totalResults']
    number_of_requests =1
    max_number_of_requests = total_results //100 + 1

    for i in range(len(latest_articles['articles'])):
       temp_articles_storage.append(latest_articles["articles"][i])
       _compare_item_url = latest_articles["articles"][i]['url']
       _compare_item_title = latest_articles["articles"][i]['title']
       _update_date =  latest_articles['articles'][i]['publishedAt'].replace("Z","")
       number_of_articles +=1
    while number_of_articles !=total_results and max_number_of_requests>number_of_requests:
        try:
            number_of_requests +=1
            all_articles = newsapi.get_everything(q='\'{0}\''.format(KEY_WORD),
                                       page_size=100,
                                       sort_by='publishedAt',
                                       to= _update_date
                                     )
        except newsapi_exception.NewsAPIException as e:
            print('News API connection error:\'{0}\''.format(e.exception['message']))
            print('\n we were unable to fully load the articles')
            return temp_articles_storage
        resp_articles =all_articles["articles"]
        _i_articles =len(resp_articles)
        if(_i_articles >0):
            for i in range(_i_articles):
                if( resp_articles[i]['url']  != _compare_item_url and  resp_articles[i]['title'] != _compare_item_title ):
                    temp_articles_storage.append(resp_articles[i])
                    _update_date = resp_articles[i]['publishedAt'].replace('Z','')
                    number_of_articles +=1
                    _compare_item_url = resp_articles[i]['url']
                    _compare_item_title = resp_articles[i]['title']

    return temp_articles_storage

newsapi = NewsApiClient(api_key=args['key'])
client = Client(args['db'],
                user='user',
                password= args['password'],
                secure=True,
                verify=False,
                database='networkapi',
                compression=True)
articles_from_api = load_all_articles()
export_articles = get_articles_with_sources(articles_from_api)
sources =get_sources_from_api()
try:
    client.execute('CREATE TABLE IF NOT EXISTS NewsAPISources ('
               'id String,name String,description String,url String,category String,language String,country String) ENGINE = MergeTree '
                'PARTITION BY id ORDER BY (id)'
    )
    client.execute('CREATE TABLE IF NOT EXISTS NewsAPIArticlesExportDB ('
               'guid String, author Nullable(String),title Nullable(String),description Nullable(String),url Nullable(String),urlToImage Nullable(String),publishedAt Nullable(String),content Nullable(String),id Nullable(String)) ENGINE = MergeTree '
                'PARTITION BY guid ORDER BY (guid)'
    )
    client.execute('ALTER TABLE NewsAPISources DELETE WHERE 1=1')
    client.execute('ALTER TABLE NewsAPIArticlesExportDB DELETE WHERE 1=1')
    client.execute('SET max_partitions_per_insert_block=5000')
    client.execute('INSERT INTO  NewsAPIArticlesExportDB VALUES',export_articles)
    client.execute('INSERT INTO  NewsAPISources VALUES',sources)
    client.execute('ALTER TABLE NewsAPISources DELETE WHERE 1=1')
    client.execute('ALTER TABLE NewsAPIArticlesExportDB DELETE WHERE 1=1')
    client.execute('SET max_partitions_per_insert_block=5000')
    client.execute('INSERT INTO  NewsAPISources VALUES',sources)
    run_sql_simple_query = client.execute(
        'Select NewsAPIArticlesExportDB.*,NewsAPISources.* from NewsAPIArticlesExportDB LEFT OUTER JOIN NewsAPISources ON NewsAPIArticlesExportDB.id = NewsAPISources.id',with_column_types=True
    )
    print('Number of items in result query \'{0}\''.format(len(run_sql_simple_query[0])))
except errors.Error as e:
    print('Check ClickHouse Log: \'{0}\''.format(e.exception['message']))
finally:
    print('\n Finished')
    client.cancel
         


