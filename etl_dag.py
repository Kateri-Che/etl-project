from airflow.decorators  import dag, task
from airflow.operators.python import get_current_context

import pandas as pd
import pandahouse as ph
import numpy as np
from datetime import timedelta, datetime

import os
from dotenv import load_dotenv

load_dotenv()

connection = {
    'host': os.getenv('CLICKHOUSE_HOST'),
    'password': os.getenv('CLICKHOUSE_PASSWORD'),
    'user': os.getenv('CLICKHOUSE_USER'),
    'database': os.getenv('CLICKHOUSE_DATABASE')
}
connection_test = {
    'host': os.getenv('CLICKHOUSE_HOST'),
    'password': os.getenv('CLICKHOUSE_TEST_PASSWORD'),
    'user': os.getenv('CLICKHOUSE_TEST_USER'),
    'database': os.getenv('CLICKHOUSE_TEST_DATABASE')
}

default_args = {
    'owner': 'e-chezhina',
    'retries': 2,
    'depends_on_past': False,
    'retry_dalay': timedelta(minutes = 2),
    'start_date': datetime(2024, 11, 14)
}

schedule_interval = '0 0 * * *'

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = True)
def my_dag_ckn():
    
    #считаем метрики ленты новостей
    @task()
    def total_lv():
        q = f'''
        SELECT toDate(time) AS event_date,
               user_id AS user,
               gender,
               age,
               os,
               CountIf(action = 'view') As views,
               CountIf(action = 'like') As likes
        FROM simulator_20241020.feed_actions
        WHERE toStartOfDay(time) = yesterday()
        GROUP BY event_date, user, gender, age, os
        '''
        feed_lv = ph.read_clickhouse(q, connection = connection)
        return feed_lv
    
    #считаем метрики ленты сообщений
    @task()
    def total_m():
        q = '''WITH get_info AS (
                   SELECT toDate(l.time) AS event_date,
                          l.receiver_id AS receiver_id,
                          COUNT(l.user_id) AS get_messages,
                          COUNT(DISTINCT l.user_id) AS senders,
                          q.gender AS gender,
                          q.age AS age,
                          q.os AS os
                    FROM simulator_20241020.message_actions AS l
                    LEFT JOIN (
                         SELECT DISTINCT user_id AS receiver_id, 
                         gender, 
                         os, 
                         age
                         FROM simulator_20241020.feed_actions
                         ) AS q ON l.receiver_id = q.receiver_id
                   WHERE toStartOfDay(l.time) = yesterday()
                   GROUP BY event_date, receiver_id, gender, os, age
                   ),

            sent_info AS (
                 SELECT toDate(time) AS event_date,
                        user_id,
                        gender,
                        age,
                        os,
                        COUNT(receiver_id) AS sent_messages,
                        COUNT(DISTINCT receiver_id) AS recipients
                FROM simulator_20241020.message_actions
                WHERE toStartOfDay(time) = yesterday()
                GROUP BY event_date, user_id, gender, age, os
                ), 

            sent_m AS(SELECT COALESCE(sent_info.event_date, get_info.event_date) AS event_date,
                             COALESCE(sent_info.user_id, get_info.receiver_id)  AS user,
                             COALESCE(sent_info.gender, get_info.gender) AS gender,
                             COALESCE(sent_info.age, get_info.age) AS age,
                             COALESCE(sent_info.os, get_info.os) AS os,
                             COALESCE(get_info.get_messages, 0) AS messages_received,
                             COALESCE(sent_info.sent_messages, 0) AS messages_sent,
                             COALESCE(sent_info.recipients, 0) AS users_received,
                             COALESCE(get_info.senders, 0) AS users_sent
                      FROM sent_info
                      LEFT JOIN get_info ON sent_info.user_id = get_info.receiver_id
                      ),

            received_m AS(SELECT COALESCE(sent_info.event_date, get_info.event_date) AS event_date,
                                 COALESCE(sent_info.user_id, get_info.receiver_id) AS user,
                                 COALESCE(sent_info.gender, get_info.gender) AS gender,
                                 COALESCE(sent_info.age, get_info.age) AS age,
                                 COALESCE(sent_info.os, get_info.os) AS os,
                                 COALESCE(get_info.get_messages, 0) AS messages_received,
                                 COALESCE(sent_info.sent_messages, 0) AS messages_sent,
                                 COALESCE(sent_info.recipients, 0) AS users_received,
                                 COALESCE(get_info.senders, 0) AS users_sent
                         FROM get_info
                         RIGHT JOIN sent_info ON get_info.receiver_id = sent_info.user_id
                         )

           SELECT DISTINCT * FROM (
                  SELECT * FROM sent_m
                  UNION ALL
                  SELECT * FROM received_m
                  ) AS combined_result'''
        
        message_m = ph.read_clickhouse(q, connection = connection)
        return message_m
    
    #объединяем датасеты
    @task
    def df_merging(feed_lv, message_m):
        df = feed_lv.merge(message_m, how = 'outer')
        df.fillna(0, inplace = True)
        return df
    
    #считаем метрики в разрезе по полу
    @task()
    def to_gender(df):
        df_gender = df.groupby('gender', as_index = False).agg({
            'event_date': 'max',
            'user': 'count',
            'views': 'sum',
            'likes': 'sum',
            'messages_received': 'sum',
            'messages_sent': 'sum',
            'users_received': 'sum',
            'users_sent': 'sum'}).rename(columns = {'gender': 'dimension_value'})
        df_gender['dimension'] = 'gender'
        return df_gender
    
    #считаем метрики в разрезе по возрасту
    @task
    def to_age(df):
        df_age = df.groupby('age', as_index = False).agg({
            'event_date': 'max',
            'user': 'count',
            'views': 'sum',
            'likes': 'sum',
            'messages_received': 'sum',
            'messages_sent': 'sum',
            'users_received': 'sum',
            'users_sent': 'sum'}).rename(columns = {'age': 'dimension_value'})
        df_age['dimension'] = 'age'
        return df_age
    
    #считаем метрики в разрезе по операционной системе
    @task
    def to_os(df):
        df_os = df.groupby('os', as_index = False).agg({
            'event_date': 'max',
            'user': 'count',
            'views': 'sum',
            'likes': 'sum',
            'messages_received': 'sum',
            'messages_sent': 'sum',
            'users_received': 'sum',
            'users_sent': 'sum'}).rename(columns = {'os': 'dimension_value'})
        df_os['dimension'] = 'os'
        return df_os
    
    #создаем финальную таблицу, объединяем данные и загружаем  
    @task
    def load(df_gender, df_age, df_os):
        q = '''
        CREATE TABLE IF NOT EXISTS test.final_dataset_10
        (
        event_date Date,
        dimension String,
        dimension_value String,
        views UInt64,
        likes UInt64,
        messages_received UInt64,
        messages_sent UInt64,
        users_received UInt64,
        users_sent UInt64
        )
        ENGINE = ReplacingMergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        '''
        ph.execute(q, connection = connection_test)
        df = pd.concat([df_gender, df_age, df_os]).astype({'views':np.uint64, 
                                                           'likes':np.uint64, 
                                                           'messages_received':np.uint64, 
                                                           'messages_sent':np.uint64,'users_received':np.uint64, 'users_sent':np.uint64})
        df = df[['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        ph.to_clickhouse(df=df, table="final_dataset_10", index=False, connection=connection_test)

    
    df = df_merging(total_lv(), total_m())
    df_gender = to_gender(df)
    df_age = to_age(df)
    df_os = to_os(df)
    load(df_os, df_gender, df_age)
    
my_dag_ckn = my_dag_ckn()
