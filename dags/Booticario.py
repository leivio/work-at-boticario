import tweepy as tw
import psycopg2


consumer_key = "ekEVFsTqzWpKggpk151ayjdDZ"
consumer_secret = "FFJIeHfNEDOwKq2aYoo3rbNGySreeEweRaYiXSZ7TjkYvlnAtO"


def datatwitter():
    auth = tw.AppAuthHandler(consumer_key, consumer_secret)
    api = tw.API(auth, wait_on_rate_limit=True)
    search_words = 'boticario AND perfumaria'
    new_search = search_words + " -filter:retweets"

    tweets = tw.Cursor(api.search,
                       q=new_search,
                       lang="pt",
                       since="2021-03-01",
                       tweet_mode='extended').items(50)
    return tweets


def get_conn_sql():
    server = "34.70.9.36"
    database = "postgres"
    username = "sqldb01"
    password = "!@#123qwe123"
    sslmode = "require"
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(server, username, database,
                                                                                 password, sslmode)
    cnn = psycopg2.connect(conn_string)
    print("Conexao OK")
    return cnn


def query_create():
    query = """ 
                    DO $$
                    BEGIN
                      if exists(select * from information_schema.tables where table_name = 'twitters') then  
                        truncate table twitters;  
                      else 
                        create table twitters (users varchar(80), locale varchar(80), texts varchar(500));
                      end if;
                    END; 
                    $$ 
                """
    return query


def query_insert():
    query = """                 
                 insert into twitters (users, locale, texts) values(%s,%s,%s)
                """
    return query



def create_table():
    sql = get_conn_sql()
    cur = sql.cursor()
    cur.execute(query_create())
    sql.commit()
    cur.close()
    sql.close()


def twitters_to_sql():
    sql = get_conn_sql()
    cur = sql.cursor()
    tweets = datatwitter()

    for tweet in tweets:
        if not tweet.retweeted:
            arg = (tweet.user.screen_name, tweet.user.location, tweet.full_text)
            cur.execute(query_insert(), arg)
            sql.commit()

    cur.close()
    sql.close()


create_table();
twitters_to_sql()


