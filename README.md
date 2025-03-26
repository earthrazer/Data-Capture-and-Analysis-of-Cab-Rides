# Data-Capture-and-Analysis-of-Cab-Rides
Suppose you are working at a mobility start-up company called ‘YourOwnCabs’ (YOC). Primarily, it is an online on-demand cab booking service. When you joined the company, it was doing around 100 rides per day. Owing to a successful business model and excellent service, the company’s business is growing rapidly, and these numbers are breaking their own records every day. YOC’s customer base and ride counts are increasing on a day-by-day basis. 


It is highly important for business stakeholders to derive quick and on-demand insights regarding the numbers to decide the company’s future strategy. Owing to the massive growth in business, it is getting tough for the company’s management to obtain the business numbers frequently, as backend MySQL is not capable of catering to all types of queries owing to large volume data. 

You, as a big data engineer, must architect and build a solution to cater to the following requirements:

1.Booking data analytics solution: This is a feature in which the ride-booking data is stored in a way such that it is available for all types of analytics without hampering business. These analyses mostly include daily, weekly and monthly booking counts as well as booking counts by the mobile operating system, average booking amount, total tip amount, etc.

2.Clickstream data analytics solution: Clickstream is the application browsing data that is generated on every action taken by the user on the app. This includes link click, button click, screen load, etc. This is relevant to know the user’s intent and then take action to engage them more in the app according to their browsing behaviour. Since this is very high-volume data (more than the bookings data), it needs to be architectured quite well and stored in a proper format for analysis.

The two types of data are the clickstream data and the batch data. The clickstream data is captured in Kafka. A streaming framework should consume the data from Kafka and load the same to Hadoop. Once the clickstream data enters the Stream processing layer, it is synced to the HDFS directory to process further. For the batch data, which is the bookings data, the data is stored in the RDS and needs to be ingested to Hadoop. 

Also, in cases wherein aggregates need to be prepared, data is read from HDFS, processed by a processing framework such as Spark and written back to HDFS to create a Hive table for the aggregated data.
Once both the data are loaded in HDFS, this data is loaded into Hive tables to make it queryable. Hive tables serve as final consumption tables for end-user querying and are eventually consistent.

First, you need to capture the clickstream data from Kafka that needs to be loaded into HDFS using a stream processing framework. Next, you need to ingest the booking data from AWS RDS to Hadoop. These are then loaded into Hive tables to make them queryable. In some cases wherein you need to prepare the aggregates, data is read from HDFS, processed by a framework such as Spark and written back to HDFS to create a Hive table for aggregated data to make it more queryable. These Hive tables serve as the final consumption tables for stakeholders to query and derive meaningful insights from the data.
