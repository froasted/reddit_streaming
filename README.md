## About the project

 - We are using the PRAW python package to fetch latest posts from a
   subbreddit.
  - The data fetched is stored on a kafka topic. 
  - We then leverage spark streaming API to process the data from the kafka
   topic in micro-batch mode. 
   - A Windowing concept is used to remove duplicate posts received under a 1 minute timeline.

### Fetch reddit credentials

 - Go to the following [URL](https://www.reddit.com/prefs/apps/])
 - Select 'create another app'
 - select type of application based on consumption of requests  (webapp/installed app/script) 
 - Copy the secrets once the application gets created. (Image for reference below)

![reddit_creds.png](images%2Freddit_creds.png)

### Prerequisite
- Make sure to have all the kafka binaries required to start a kafka server locally. You can get it from [here](https://kafka.apache.org/downloads)
- Export the variable `$KAFKA_HOME` so that it points to the path where all the kafka binaries are stored.

![kafka_home_setup.gif](images%2Fkafka_home_setup.gif)
  
 ### Setup
 - Add the credentials fetched before in [credentials.ini](credentials.ini). (We can use any user name for populating the `USER_AGENT` variable)
 - Create a python virtual environment  : `python3 -m venv venv`, 
 - Fetch all the required python packages :  `pip3 install -r requirements.txt` (P.S : python version in my machine was 3.10.1)
 
 ### Execute the job
 - Run the script [start_zookeeper.sh](kafka_scripts%2Fstart_zookeeper.sh) and  [start_kafka_server.sh](kafka_scripts%2Fstart_kafka_server.sh) to start the kafka server. By default it gets launched on localhost:9092.
 - Run the script [create_topic.sh](kafka_scripts%2Fcreate_topic.sh) to create a kafka topic to store metadata.
 Example : `bash create_topic.sh redditpost`
 - Run the file [reddit_post_producer.py](src%2Freddit_post_producer.py) :
 `python3 reddit_post_producer.py -creds credentials.ini -sub <subreddit_name> -b <kafka server host:port> -t <kafka topic name>`
 Example : `python3 reddit_post_producer.py -creds credentials.ini -sub AskReddit -b localhost:9092 -t redditpost`
 - Finally, run the file [reddit_stream.py](src%2Freddit_stream.py). It would fetch the data from kafka and perform the required pre-processing and then output the data to console.

### Clean up 
- Once the streaming job ends, make sure to delete the checkpoint directory. Otherwise we might face errors on stream restarts.
- Delete the data from kafka topic once done with processing using the [delete_topic_data.sh](kafka_scripts%2Fdelete_topic_data.sh).
Example : `bash delete_topic_data.sh localhost:9092 redditpost`

#### Extras

- Used black to format all the python files `black . --exclude=venv`
  

