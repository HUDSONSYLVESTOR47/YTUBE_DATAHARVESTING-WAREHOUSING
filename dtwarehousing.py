#IMPORT PACKAGE

from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#API KEY CONNECTION

def Api_connect():
    Api_Id="AIzaSyCdZo_f9k9yhda4FqdZV3OZeYdVz-REV9A"

    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_Id)

    return youtube

youtube=Api_connect()

#GET CHANNEL INFORMATION
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for index in response['items']:
        data=dict(Channel_Name=index['snippet']['title'],
                Channel_Id=index['id'],
                Subscribers= index['statistics']['subscriberCount'],
                Views= index['statistics']['viewCount'],
                Total_Videos= index['statistics']['videoCount'],
                Channel_Description= index['snippet']['description'],
                Playlist_Id= index['contentDetails']['relatedPlaylists']['uploads'])
    
    return data

#Acquiring video ids

def get_videos_ids(channel_id):

    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token= None

    while True:
        response1=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
        for num in range(len(response1['items'])):
            video_ids.append(response1['items'][num]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    
    return video_ids

#GET VIDEO INFORMATION

def get_video_info(Video_ids):

    video_data=[]

    for video_id in Video_ids:
        request=youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )
        response=request.execute()

        for index in response['items']:
            data=dict(Channel_Name=index['snippet']['channelTitle'],
                    Channel_Id=index['snippet']['channelId'],
                    Video_Id=index['id'],
                    Title=index['snippet']['title'],
                    Tags=index['snippet'].get('tags'),
                    Thumbnail=index['snippet']['thumbnails']['default']['url'],
                    Description=index['snippet'].get('description'),
                    Date_of_Publish=index['snippet']['publishedAt'],
                    Duration=index['contentDetails']['duration'],
                    Views=index['statistics'].get('viewCount'),
                    Likes=index['statistics'].get('likeCount'),
                    Comments=index['statistics'].get('commentCount'),
                    Favorite_Count=index['statistics']['favoriteCount'],
                    Definition=index['contentDetails']['definition'],
                    Caption_Status=index['contentDetails']['caption'])
            video_data.append(data)

    return video_data

#GET COMMENT INFO
def get_comment_info(Video_ids):

    Comment_Data=[]
    try:
        for video_id in Video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50,
            )
            response=request.execute()

            for index in response['items']:
                data=dict(Comment_Id=index['snippet']['topLevelComment']['id'],
                        Video_Id=index['snippet']['videoId'],
                        Comment_Text=index['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=index['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Time_of_Comment=index['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                
                Comment_Data.append(data)
    except:
        pass

    return Comment_Data

#GET PLAYLIST DETAILS

def get_playlist_details(channel_id):

    next_page_token=None
    Playlist_data=[]
    while True:
        request=youtube.playlists().list(
            part='snippet, contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for index in response['items']:
            data=dict(
                Playlist_Id=index['id'],
                Playlist_Title=index['snippet']['title'],
                Channel_Id=index['snippet']['channelId'],
                Channel_Name=index['snippet']['channelTitle'],
                PublishedAt=index['snippet']['publishedAt'],
                Video_Count=index['contentDetails']['itemCount']
            )
            Playlist_data.append(data)

        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break

    return Playlist_data

#UPLOAD TO MONGODB

client=pymongo.MongoClient("mongodb+srv://31sylvestor01:hudson@sylvestor.s7c1qro.mongodb.net/?retryWrites=true&w=majority")
db=client['Youtube_data']

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_information":vi_details,"comment_information":com_details})
    return "upload status: success"

#SQL CURSOR CONNECTION

mydb=psycopg2.connect(host='localhost',
                            user='postgres',
                            password='Hudson47*',
                            database='youtube_data',
                            port='5432')
cursor=mydb.cursor()

#TABLE CREATION AND MULTI-ROW INSERTION:

#CHANNEL TABLE CREATION

def channels_table():

    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Hudson47*',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channel Table already created")

    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df=pd.DataFrame(ch_list)

    for index,row in df.iterrows():
            insert_query='''insert into channels(Channel_Name ,
                                                Channel_Id ,
                                                Subscribers ,
                                                Views ,
                                                Total_Videos ,
                                                Channel_Description ,
                                                Playlist_Id)
                                                
                                                values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'])
                
            try:
                cursor.execute(insert_query,values)
                mydb.commit()

            except:
                print("Channel values already inserted or your code has some error")

#PLAYLIST TABLE CREATION

def playlists_table():

    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Hudson47*',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Playlist_Title varchar(80) ,
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=pd.DataFrame(pl_list)

    for index,row in df1.iterrows():
        insert_query='''insert into playlists(Playlist_Id ,
                                        Playlist_Title ,
                                        Channel_Id ,
                                        Channel_Name ,
                                        PublishedAt ,
                                        Video_Count
                                        )
                                        
                                        values(%s,%s,%s,%s,%s,%s)'''
        values=(row['Playlist_Id'],
                row['Playlist_Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count']
                )
        

        cursor.execute(insert_query,values)
        mydb.commit()

#VIDEO TABLE CREATION

def videos_table():

    mydb=psycopg2.connect(host='localhost',
                            user='postgres',
                            password='Hudson47*',
                            database='youtube_data',
                            port='5432')
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                            Channel_Id varchar(100),
                                            Video_Id varchar(30) primary key,
                                            Title varchar(150),
                                            Tags text,
                                            Thumbnail varchar(200),
                                            Description text,
                                            Date_of_Publish timestamp,
                                            Duration interval,
                                            Views bigint,
                                            Likes bigint,
                                            Comments bigint,
                                            Favorite_Count int,
                                            Definition varchar(10),
                                            Caption_Status varchar(50))'''
    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data['video_information'][i])
    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
        insert_query='''insert into videos(Channel_Name ,
                                        Channel_Id ,
                                        Video_Id ,
                                        Title ,
                                        Tags ,
                                        Thumbnail ,
                                        Description ,
                                        Date_of_Publish ,
                                        Duration ,
                                        Views ,
                                        Likes ,
                                        Comments ,
                                        Favorite_Count ,
                                        Definition ,
                                        Caption_Status
                                        )
                                        values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Video_Id'],
                row['Title'],
                row['Tags'],
                row['Thumbnail'],
                row['Description'],
                row['Date_of_Publish'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['Comments'],
                row['Favorite_Count'],
                row['Definition'],
                row['Caption_Status']
                        )

        cursor.execute(insert_query,values)
        mydb.commit()

#COMMENTS TABLE CREATION

def comments_table():
        
    mydb=psycopg2.connect(host='localhost',
                                user='postgres',
                                password='Hudson47*',
                                database='youtube_data',
                                port='5432')
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(100),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Time_of_Comment timestamp
                                                        )'''


    cursor.execute(create_query)
    mydb.commit()

    com_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data['comment_information'][i])
    df3=pd.DataFrame(com_list)

    for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id ,
                                        Video_Id ,
                                        Comment_Text ,
                                        Comment_Author ,
                                        Time_of_Comment
                                        )
                                        
                                        values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Time_of_Comment']
                )
        
        cursor.execute(insert_query,values)
        mydb.commit()

#SQL TABLE CREATION FUNCTIONS CALL

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()

    return "Tables created successfully"

#CHANNELS @ STREAMLIT

def show_channels_table():
    ch_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)

    return df

#PLAYLISTS @ STREAMLIT

def show_playlists_table():
    pl_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data['playlist_information'][i])
    df1=st.dataframe(pl_list)

    return df1

#VIDEOS @ STREAMLIT

def show_videos_table():
        vi_list=[]
        db=client['Youtube_data']
        coll1=db['channel_details']
        for vi_data in coll1.find({},{"_id":0,"video_information":1}):
                for i in range(len(vi_data["video_information"])):
                        vi_list.append(vi_data['video_information'][i])
        df2=st.dataframe(vi_list)

        return df2

#COMMENTS @ STREAMLIT

def show_comments_table():
    com_list=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data['comment_information'][i])
    df3=st.dataframe(com_list)

    return df3

#STREAMLIT PART CODING

with st.sidebar:
    st.title(":black[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Takeaway")
    st.caption("Python script")
    st.caption("Api Integration")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("Data Management- MongoDB and SQL")

channel_id=st.text_input("Enter the channel id:")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client['Youtube_data']
    coll1=db['channel_details']
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append(ch_data['channel_information']['Channel_Id'])
    
    if channel_id in ch_ids:
        st.success("The channel details of channel id already exists")
    
    else:
        insert=channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table= tables()
    st.success(Table)

show_table=st.radio("CLICK THE ANY OF THE FOLLOWING OPTIONS TO VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

#POSTGRES SQL CONNECTION

mydb=psycopg2.connect(host='localhost',
                            user='postgres',
                            password='Hudson47*',
                            database='youtube_data',
                            port='5432')
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. What are all the names of all videos and their corresponding channel?",
                                              "2. Which channel has most videos and what is the name of the channel?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video and what are their corresponding video name?",
                                              "5. Which videos have highest number of likes, what are their corresponding channel names?",
                                              "6. What is the total number of likes and dislikes for each video, and what is their video name?",
                                              "7. What is total number of views for each channel and what are their channel name?",
                                              "8. What are the names of channel that has released video in the year 2022",
                                              "9. What is the average duration of all videos in each channel and what are their channel names",
                                              "10.Which videos have highest number of comments, what are their channel names?"))

if question=="1. What are all the names of all videos and their corresponding channel?":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    ans1=cursor.fetchall()
    st1=pd.DataFrame(ans1,columns=["video name","channel name"])
    st.write(st1)

elif question=="2. Which channel has most videos and what is the name of the channel?":
    query2='''select channel_name as channelname, total_videos as no_videos from channels order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    ans2=cursor.fetchall()
    st2=pd.DataFrame(ans2,columns=["channel name","no. of videos"])
    st.write(st2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views, channel_name as channelname, title as videotitle from videos where views is not null
                order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    ans3=cursor.fetchall()
    st3=pd.DataFrame(ans3,columns=["views","channel name","video title"])
    st.write(st3)

elif question=="4. How many comments were made on each video and what are their corresponding video name?":
    query4='''select comments as no_comments, title as videotitle from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    ans4=cursor.fetchall()
    st4=pd.DataFrame(ans4,columns=["no of comments","video title"])
    st.write(st4)

elif question=="5. Which videos have highest number of likes, what are their corresponding channel names?":
    query5='''select title as videotitle, channel_name as channelname, likes as likecount
            from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    ans5=cursor.fetchall()
    st5=pd.DataFrame(ans5,columns=["video title","channel name","like count"])
    st.write(st5)

elif question=="6. What is the total number of likes and dislikes for each video, and what is their video name?":
    query6='''select likes as likecount, title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    ans6=cursor.fetchall()
    st6=pd.DataFrame(ans6,columns=["like count","video title"])
    st.write(st6)

elif question=="7. What is total number of views for each channel and what are their channel name?":
    query7='''select channel_name as channelname, views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    ans7=cursor.fetchall()
    st7=pd.DataFrame(ans7,columns=["channel name","total views"])
    st.write(st7)

elif question=="8. What are the names of channel that has released video in the year 2022":
    query8='''select title as video_title, date_of_publish as releasedate, channel_name as channelname from videos
            where extract(year from date_of_publish)=2022'''
    cursor.execute(query8)
    mydb.commit()
    ans8=cursor.fetchall()
    st8=pd.DataFrame(ans8,columns=["video title","release date","channel name"])
    st.write(st8)

elif question=="9. What is the average duration of all videos in each channel and what are their channel names":
    query9='''select channel_name as channelname, AVG(duration) as averageduration from videos
            group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    ans9=cursor.fetchall()
    st9=pd.DataFrame(ans9,columns=["channelname","averageduration"])

    list9=[]
    for index,row in st9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        list9.append(dict(channeltitle=channel_title, averageduration=average_duration_str))

    st9=pd.DataFrame(list9)
    st.write(st9)

elif question=="10.Which videos have highest number of comments, what are their channel names?":
    query10='''select title as videotitle, channel_name as channelname, comments as comments from videos 
            where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    ans10=cursor.fetchall()
    st10=pd.DataFrame(ans10,columns=["video title","channel name","comments"])
    st.write(st10)      