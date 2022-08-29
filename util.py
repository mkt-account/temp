import os
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from io import StringIO, BytesIO
import boto3
from pyathena import connect
from grist_api import GristDocAPI
from urllib import parse
from datetime import datetime, timedelta
import time
import json


# 환율 데이터 가져오기(yyyy-mm-dd)
def get_exchange(start_date, end_date):
    try:
        # AWS Athena config
        cursor = connect(aws_access_key_id='AKIAVUNY43ERE42JR62G',
                     aws_secret_access_key='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',
                     s3_staging_dir='s3://aws-athena-query-results-387471694114-ap-northeast-2/',
                     region_name='ap-northeast-2',
                     schema_name='marketing').cursor()
        query = f"""select date, exchange from marketing.exchange where date between '{start_date}' and '{end_date}' """
        cursor.execute(query)
        exchange_rate = cursor.fetchall()
        cursor.close()
        exchange_rate_df = pd.DataFrame(exchange_rate, columns=['date', 'exchange_rate'])
        return exchange_rate_df
    except Exception as e:
        print(f'에러발생 {e}')


# 전체 시트 데이터 가져오기
def get_sheet(spread, sheet):
    try:
        # gspread config
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('google_api(from_qq).json', scope)
        gc = gspread.authorize(credentials)
        gc_target = gc.open(spread).worksheet(sheet)
        raw_data = gc_target.get_all_values()
        rd_df = pd.DataFrame(raw_data)
        rd_df.rename(columns=rd_df.iloc[0], inplace=True)
        rd_df.drop(rd_df.index[0], inplace=True)
        return rd_df
    except Exception as e:
        print(f'에러발생 {e}')


# S3에 기록하기
def write_to_s3(target, path, filename):
    try:
        # AWS S3 config
        s3 = boto3.client(
            's3',
            aws_access_key_id='AKIAVUNY43ERDEZ6MIOT',
            aws_secret_access_key='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'
        )
        csv_buffer = StringIO()
        target.to_csv(csv_buffer, sep=",", na_rep="", index=False, header=False, encoding="utf-8-sig")
        s3.put_object(
            Bucket='bucketplace-mkt',
            Key=f'{path}/{filename}',
            Body=csv_buffer.getvalue(),
        )
    except Exception as e:
        print(f'에러발생 {e}')


# S3에 기록하기
def write_to_s3_bucket(target, bucket, path, filename):
    try:
        # AWS S3 config
        s3 = boto3.client(
            's3',
            aws_access_key_id='AKIAVUNY43ERDEZ6MIOT',
            aws_secret_access_key='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'
        )
        csv_buffer = StringIO()
        target.to_csv(csv_buffer, na_rep="", index=False, encoding="utf-8-sig")
        # target.to_csv(csv_buffer, sep=",", na_rep="", index=False, encoding="utf-8-sig")
        s3.put_object(
            Bucket=bucket,
            Key=f'{path}/{filename}',
            Body=csv_buffer.getvalue(),
        )
    except Exception as e:
        print(f'에러발생 {e}')


# S3에 기록하기(parquet로 변환)
def write_to_s3_parquet(target, path, filename):
    try:
        # AWS S3 config
        s3 = boto3.client(
            's3',
            aws_access_key_id='AKIAVUNY43ERDEZ6MIOT',
            aws_secret_access_key='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'
        )
        buffer = BytesIO()
        target.to_parquet(buffer, engine="pyarrow", compression="gzip")
        buffer.seek(0)
        s3.put_object(
            Bucket='bucketplace-mkt',
            Key=f'{path}/{filename}',
            Body=buffer,
        )
        print(f'{filename} 기록 완료')
    except Exception as e:
        print(f'에러발생 {e}')


# Grist에 기록하기
def write_to_grist(target, table):
    try:
        # Grist config
        SERVER = "https://bucketplace.getgrist.com"
        DOC_ID = "rDVg5isWwB9Y"
        os.environ['GRIST_API_KEY'] = "3e31fe01718e131eebecebe3f87e4f3c2d8e89e5"
        api = GristDocAPI(DOC_ID, server=SERVER)
        # RECORD 기준 딕셔너리로 변경
        target_dict = target.to_dict('records')
        api.add_records(table, target_dict, chunk_size=5000)
    except Exception as e:
        print(f'에러발생 {e}')


# Grist 특정 테이블 전체 삭제하기
def delete_all_grist(table):
    try:
        # Grist config
        SERVER = "https://bucketplace.getgrist.com"
        DOC_ID = "rDVg5isWwB9Y"
        os.environ['GRIST_API_KEY'] = "3e31fe01718e131eebecebe3f87e4f3c2d8e89e5"
        api = GristDocAPI(DOC_ID, server=SERVER)
        # fetch
        content = api.fetch_table(table)
        # delete
        ids = list(i.id for i in content)
        api.delete_records(table, ids)
    except Exception as e:
        print(f'에러발생 {e}')


# Grist에서 특정 일자 데이터 삭제하기
# delete_period_grist 사용하려면 event_timestamp(string) 반드시 필요함
def delete_period_grist(table, date_col_name, start_date, end_date):
    try:
        # Grist config
        DOC_ID = "rDVg5isWwB9Y"
        os.environ['GRIST_API_KEY'] = "3e31fe01718e131eebecebe3f87e4f3c2d8e89e5"
        # fetch
        header = {
            "Authorization": "Bearer 3e31fe01718e131eebecebe3f87e4f3c2d8e89e5"
        }
        request_fetch_url = f'https://bucketplace.getgrist.com/api/docs/{DOC_ID}/tables/{table}/records'
        start_date_format = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_format = datetime.strptime(end_date, '%Y-%m-%d')
        date_diff = end_date_format - start_date_format
        date_list = []
        for e in range(0, int(date_diff.days) + 1, 1):
            target_date_format = start_date_format + timedelta(days=e) + timedelta(hours=9)
            target_date_timestamp = time.mktime(target_date_format.timetuple())
            date_list.append(str(int(target_date_timestamp)))
        filter_params = '{' + '"' + date_col_name + '"' + ':' + json.dumps(date_list) + '}'
        param_encoded = parse.quote(filter_params)
        request_fetch = requests.get(f'{request_fetch_url}?filter={param_encoded}', headers=header)
        result_fetch = json.loads(request_fetch.text)
        result_df = pd.DataFrame().from_dict(result_fetch['records'], orient='columns')
        if not result_df.empty:
            id_list = list(result_df['id'])
            # delete
            request_delete_url = f'https://bucketplace.getgrist.com/api/docs/{DOC_ID}/tables/{table}/data/delete'
            request_delete = requests.post(f'{request_delete_url}?filter={param_encoded}', headers=header, json=id_list)
            print(id_list, "삭제 완료")
        else:
            print("삭제할 ID가 없음")

    except Exception as e:
        print(f'에러발생 {e}')


# Grist에서 가져오기
def get_grist(table_name, filter_condition):
    GRIST_API_KEY = "3e31fe01718e131eebecebe3f87e4f3c2d8e89e5"
    grist_header = {
        "Authorization": "Bearer " + GRIST_API_KEY
    }
    doc_id = "rDVg5isWwB9Y"
    filter_encoded = parse.quote(filter_condition)
    url = f'https://bucketplace.getgrist.com/api/docs/{doc_id}/tables/{table_name}/records?filter={filter_encoded}'
    grist_request = requests.get(url=url, headers=grist_header)
    if grist_request.status_code == 200:
        print(grist_request)
        grist_response = grist_request.json()
        result = grist_response['records']
        print(f'{len(result)}개행의 데이터 있음')
        return result
    elif grist_request.status_code != 404:
        print(grist_request)
        time.sleep(10)
        grist_request = requests.get(url=url, headers=grist_header)
        grist_response = grist_request.json()
        result = grist_response['records']
        print(f'{len(result)}개행의 데이터 있음')
        return result
    else:
        print(grist_request)


def get_list_s3(bucket_name, prefix):
    # AWS S3 config
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id='AKIAVUNY43ERDEZ6MIOT',
        aws_secret_access_key='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'
    )
    my_bucket = s3_resource.Bucket(bucket_name)

    objects_list = []
    for my_bucket_object in my_bucket.objects.filter(Prefix=prefix):
        objects_list.append(my_bucket_object.key)
    return objects_list


def get_object_s3(bucket_name, filename):
    # AWS S3 config
    s3_client = boto3.client(
        's3',
        aws_access_key_id='AKIAVUNY43ERDEZ6MIOT',
        aws_secret_access_key='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'
    )
    response = s3_client.get_object(Bucket=bucket_name, Key=filename)
    file_stream = response['Body']
    # data_csv = pd.read_csv(response['Body'])
    return file_stream


def add_partition_athena(partition_date_str, table_name, location):
    try:
        # AWS S3 config
        s3_client = boto3.client(
            'athena',
            aws_access_key_id='AKIAVUNY43ERDEZ6MIOT',
            aws_secret_access_key='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',
            region_name='ap-northeast-2'
        )
        sql = f"""ALTER TABLE {table_name} ADD IF NOT EXISTS PARTITION (base_dt='{partition_date_str}') location '{location}';"""
        response = s3_client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
                'Database': 'marketing'
            },
            ResultConfiguration={
                'OutputLocation': 's3://bucketplace-athena-result/'
            }
        )
        print(response)

    except Exception as e:
        print(f'에러발생 {e}')
