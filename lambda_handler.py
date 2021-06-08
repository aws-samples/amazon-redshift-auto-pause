#!/usr/bin/python
import os, boto3, time

def lambda_handler(event, context):
    redshift_data_client = boto3.client("redshift-data")
    redshift_admin_client = boto3.client("redshift")
    redshift_cluster_id = event['redshift_cluster_id']
    redshift_database = event['redshift_database']
    redshift_user = event['redshift_user']
    region = context.invoked_function_arn.split(":")[3]
    account = context.invoked_function_arn.split(":")[4]
    sns_topic = event['sns_topic']
    clusterStatusResponse = redshift_admin_client.describe_clusters(ClusterIdentifier=redshift_cluster_id)
    clusterStatus = clusterStatusResponse['Clusters'][0]['ClusterStatus']
    if clusterStatus == "paused":
        print("Cluster {} is already in paused state".format(redshift_cluster_id))
    else:  
        sql_statement = "\
            create table if not exists autopause_log ( \
                log_ts datetime, \
                query_cnt int, \
                status varchar(10));"
        query_id = redshift_data_client.execute_statement(Database=redshift_database, DbUser=redshift_user,Sql=sql_statement, ClusterIdentifier=redshift_cluster_id)["Id"]

        sql_statement = "\
            select count(1) query_cnt, current_timestamp AT TIME ZONE 'UTC' log_ts \
            from stv_inflight \
            where userid != 1 \
            and text not like '%stv_inflight%';"
        query_id = redshift_data_client.execute_statement(Database=redshift_database, DbUser=redshift_user,Sql=sql_statement, ClusterIdentifier=redshift_cluster_id)["Id"]

        statuslist = ["SUBMITTED", "PICKED", "STARTED"]
        while (redshift_data_client.describe_statement(Id=query_id)["Status"] in statuslist) :
            time.sleep(3)

        query_cnt = redshift_data_client.get_statement_result(Id=query_id)["Records"][0][0]["longValue"]
        log_ts = redshift_data_client.get_statement_result(Id=query_id)["Records"][0][1]["stringValue"]
        if (query_cnt==0):
            status = "Pause"
            print("No activity in cluster {}".format(redshift_cluster_id))
        else :
            status = "Active"
            print(str(query_cnt) + " queries are running")
        sql_statement = "insert into autopause_log (log_ts,query_cnt,status) values ('"+log_ts+"',"+str(query_cnt)+",'"+status+"');"
        redshift_data_client.execute_statement(Database=redshift_database, DbUser=redshift_user,Sql=sql_statement, ClusterIdentifier=redshift_cluster_id)

        if (query_cnt == 0):
            print("Pausing the cluster {}".format(redshift_cluster_id))
            if (sns_topic != ""):
                print("Sending SNS Notification to topic: " + sns_topic)
                topic = "arn:aws:sns:"+region+":"+account+":"+sns_topic
                sns = boto3.client("sns")
                sns.publish(
                    TopicArn=topic,
                    Message="There was no activity as of "+log_ts+" in Redshift Cluster: "+redshift_cluster_id+" and it will be paused.",
                    Subject="Pausing cluster: " + redshift_cluster_id)
            redshift_admin_client.pause_cluster(ClusterIdentifier=redshift_cluster_id)