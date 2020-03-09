import boto3
import requests
import re
from pyspark import SparkContext
sc = SparkContext.getOrCreate()


def extract_info(info):
    xResults = []
    for inf in info:
        tweet_id = inf[0]
        user = inf[1].replace("\t", " ").split(" ")
        screen_name = user[-1]
        username = " ".join(user[0:-1])
        tweet_text = inf[2]
        follower = inf[3]
        locate = inf[4].replace("\t", " ").split(" ")
        geo = locate[-1]
        location = " ".join(locate[:-1])
        date = inf[5]
        
        xResults.append(
            {
            'tweet_id': tweet_id,
            'screen_name': screen_name,
            'username': username,
            'tweet_text': tweet_text,
            'follower_count': follower,
            'location': location,
            'geo': geo,
            'date': date
            }
        )
    return xResults


BucketName = ''
ACCESS_KEY = ""
SECRET_KEY = ""
REGION = 'us-east-1'
regex = r'(\d{19})\s+(.*)\s+RT\s@[a-zA-Z]+:(.*?)\s+(\d+)\s+([A-Za-z\t\s]+)\s+([a-zA-Z]{3}\s[a-zA-Z]{3}\s\d{2}\s\d{2}:\d{2}:\d{2}\s\+\d{4}\s\d{4})'

if __name__ == "__main__":
    s3 = boto3.client('s3', region_name = REGION, aws_access_key_id = ACCESS_KEY,
                        aws_secret_access_key = SECRET_KEY)
    content = []
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket = BucketName)
    date_regex = ''
    count = 0
    for page in page_iterator:
        for p in page.get("Contents", []):
            count += 1
            print("INFO: {} Processing key {}".format(count, p["Key"]))
            body = s3.get_object(Bucket=BucketName, Key=p["Key"])['Body'].read().decode()
            '''      
            tweet_id = re.findall(r'\d{19}', body)
            username = re.findall(r'\d{19}(.*)\s+RT', body)
            tweet_text = re.findall(r'RT\s@[a-zA-Z]+\s?:(.*?)\s+\d+',body)
            date = re.findall(r'[a-zA-Z]{3}\s[a-zA-Z]{3}\s\d{2}\s\d{2}:\d{2}:\d{2}\s\+\d{4}\s\d{4}', body)
            info = re.findall(r'RT\s@[a-zA-Z]+\s?:(.*?)\s+(\d+)\s+(.*)\s+[a-zA-Z]{3}\s[a-zA-Z]{3}\s\d{2}\s\d{2}:\d{2}:\d{2}\s\+\d{4}\s\d{4}', body)
            
            usern = [user.replace("\t", ' ') for user in username]      
            
            
            if tweet_id != []:
                #for i_n in info:  
                content += [{"tweet_id": TweetId, "tweet_text": TweetText, "tweet_date": TweetDate, "screenname": name.split(" ")[-1], "username": " ".join(name.split(" ")[0:-1])} for TweetId, TweetText, TweetDate, name in zip(tweet_id, tweet_text, date, usern)]
            print(body)
            '''
            info = re.findall(regex, body)
            if info != []:
                content += extract_info(info)
    df = sc.parallelize(content).toDF()
    
    query = """
    DROP TABLE IF EXISTS twitter
    """
    spark.sql(query)
    df.write.saveAsTable("twitter")