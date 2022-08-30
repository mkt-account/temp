import boto3
import zipfile
import json
import pandas as pd
import io
from datetime import datetime
from pytz import timezone
from util import get_sheet, get_list_s3, write_to_s3_parquet
import time
from multiprocessing import Pool
from itertools import chain

s3 = boto3.resource(
        's3',
        aws_access_key_id='AWS-ACCESS-KEY-ID',
        aws_secret_access_key='AWS-SECRET-ACCESS-KEY'
)
Bucket = 'bucketplace-mkt'
today = datetime.now(timezone('Asia/Seoul'))
today_str = today.strftime('%Y-%m-%d')
today_s3_str = today.strftime('%Y%m%d')


def get_segments_info():
    request_df = get_sheet('Braze_segment_sync요청시트', '요청')
    segment_info_df = request_df[['segment-api', 'canvas-api', 'from', 'to']]
    segment_info_dict = segment_info_df.to_dict('list')
    segment_info = []
    for e in range(0, len(segment_info_dict['segment-api']), 1):
        element = {
            'segment_id': segment_info_dict['segment-api'][e],
            'canvas_ids': list(segment_info_dict['canvas-api'][e].split(","))
            if segment_info_dict['canvas-api'][e] else [],
            'from': segment_info_dict['from'][e],
            'to': segment_info_dict['to'][e]
        }
        segment_info.append(element)
    return segment_info


# 압축 파일 하나에 대한 작업
def zipfile_job(target_zipfile):
    print(f'target_zipfile : {target_zipfile}')
    try:
        file_raw = s3.Object(Bucket, target_zipfile)
        file_response = file_raw.get()['Body'].read()
        buffer = io.BytesIO(file_response)
        z = zipfile.ZipFile(buffer)
        files_list = []
        for i in range(0, len(z.namelist()), 1):
            # external_id 기준으로 기록, 보통 5000 rows
            files = z.open(z.infolist()[i])
            # external_id 기준 레코드 내에서 다시 step 으로 나누기 위함
            for file in files:
                file_data = file.decode('utf-8')
                file_data = file_data.strip('\n')
                file_dict_original = json.loads(file_data)
                file_dict = {
                    'external_id': file_dict_original['external_id'] if 'external_id' in file_dict_original else '',
                    'campaigns_received': '',
                    'canvases_received': '',
                    'steps_received': ''
                }
                if 'campaigns_received' in file_dict_original and len(file_dict_original['campaigns_received']) > 0:
                    campaigns_list = list()
                    for c in file_dict_original['campaigns_received']:
                        campaigns_list.append(c['api_campaign_id'])
                    file_dict['campaigns_received'] = campaigns_list
                if 'canvases_received' in file_dict_original and len(file_dict_original['canvases_received']) > 0:
                    canvases_list = list()
                    for c in file_dict_original['canvases_received']:
                        canvases_list.append(c['api_canvas_id'])
                    file_dict['canvases_received'] = canvases_list
                if 'canvases_received' in file_dict_original and len(file_dict_original['canvases_received']) > 0:
                    steps_list = list()
                    for c in file_dict_original['canvases_received']:
                        if 'steps_received' in c and len(c['steps_received']) > 0:
                            for step in c['steps_received']:
                                steps_list.append(step)
                    file_dict['steps_received'] = steps_list

                files_list.append(file_dict)
        z.close()
        return files_list
    except Exception as e:
        print(f'에러발생 {e}')


# 타겟 캔버스를 수신한 경우에만 스텝 단위로 row 만들기
# external_id 단위의 작업임
def after_zipfile_job(file_dict, canvas_id_list):
    user_list = list()

    # 캔버스 수신 이력이 있는 경우만 수행
    if file_dict['canvases_received'] != '' or len(file_dict['canvases_received']) > 0:
        for canvas in file_dict['canvases_received']:
            # 수신한 캔버스 중 타겟 캔버스 수신한 이력이 있는 경우만 수행
            if canvas in canvas_id_list:
                # 스텝이 있는 경우만 수행
                if file_dict['steps_received'] != '' or len(file_dict['steps_received']) > 0:

                    # 캔버스 내 스텝별로 row를 생성
                    for step in file_dict['steps_received']:
                        user_object = {
                            'base_dt': today_str,
                            'user_id': file_dict['external_id'] if 'external_id' in file_dict else "",
                            'step_id': step['api_canvas_step_id'] if 'api_canvas_step_id' in step else "",
                            'step_nm': step['name'] if 'name' in step else "",
                            'entry_time': step['last_received'] if 'last_received' in step else "",
                            # entry_count 확인 필요
                            'entry_count': 1
                        }
                        object_df = pd.DataFrame(data=user_object, index=[0])

                        # 수신한 캠페인 리스트 있다면 붙여주기
                        if file_dict['campaigns_received'] != "" or len(file_dict['campaigns_received']) > 0:
                            object_df['campaigns_received_ids'] = ",".join(file_dict['campaigns_received'])
                        else:
                            object_df['campaigns_received_ids'] = ""

                        user_list.append(object_df)

    if len(user_list) > 0:
        user_df = pd.concat(user_list)
        user_df.reset_index(inplace=True, drop=True)
        return user_df


def list_chunk(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]


def callback_func(result):
    if len(result) > 0:
        result_not_none = [x for x in result if x is not None]
        if len(result_not_none) > 0:
            segment_result = pd.concat(result_not_none)
            segment_result.reset_index(inplace=True, drop=True)
            segment_result['segment_id'] = s["segment_id"]
            segment_result['target_canvas_id'] = ",".join(s["canvas_ids"]) if s["canvas_ids"] is not None else ""
            segment_result = segment_result[
                ['base_dt', 'user_id', 'step_id', 'step_nm', 'entry_time', 'entry_count',
                 'campaigns_received_ids', 'target_canvas_id', 'segment_id']]
            filepath = f"braze/user_data/segment-export-table/date={today_s3_str}"
            current_time_str = datetime.now(timezone('Asia/Seoul')).strftime('%HH%MM%SS')
            filename = f"braze_steps_users_detail_{s['segment_id']}_{today_s3_str}_{current_time_str}.parquet"
            segment_result['entry_count'] = segment_result['entry_count'].astype(str)
            # 아래 함수는 완료나 실패시 메시지 프린트함
            write_to_s3_parquet(segment_result, filepath, filename)
        else:
            print("second_jobs_result 없음")
    else:
        print("len(result)가 0")


if __name__ == '__main__':
    pool_num = 3
    segments_info_original = get_segments_info()
    segments_info = list()
    for seg in segments_info_original:
        # 날짜 필터링
        if seg['from'] <= today_str <= seg['to']:
            # 캔버스 ID 여부 필터링
            if len(seg['canvas_ids']) > 0:
                segments_info.append(seg)
            else:
                print(f'{seg["segment_id"]} : 적재 기간에 해당하나 필터링할 캔버스 ID 없음')
        else:
            print(f'{seg["segment_id"]} : 적재 기간에 해당하지 않음')

    # 세그먼트 마다 반복
    for s in segments_info:
        print(f'segment_id : {s["segment_id"]}')
        start_time = time.time()
        prefix = f'braze/user_data/segment-export/{s["segment_id"]}/{today_str}/'
        objects = get_list_s3('bucketplace-mkt', prefix)
        print(f'압축파일 : {len(objects)}개')

        # 테스트용
        objects = objects[0:2]

        if len(objects) <= 150:
            p = Pool(pool_num)
            first_job = p.map_async(zipfile_job, objects)
            first_job_results = list(chain(*first_job.get()))
            p.close()
            p.join()

            # tuple list
            second_job_list = list()
            for f in first_job_results:
                second_job = (f, s['canvas_ids'])
                second_job_list.append(second_job)

            second_job_list_chunk = list_chunk(second_job_list, 10000)
            for jobs in second_job_list_chunk:
                start_time_jobs = time.time()
                print(f'after_zipfile_job, {len(jobs)}개 / {len(second_job_list)}개 실행...')
                p = Pool(pool_num)
                second_jobs = p.starmap_async(after_zipfile_job, [s for s in jobs], callback=callback_func)
                p.close()
                p.join()
                print(f'time: {(time.time() - start_time_jobs)}초')
            print(f'{s["segment_id"]} time: {(time.time() - start_time) / 60}분')

        elif 150 < len(objects) <= 300:
            p = Pool(pool_num)
            first_job_1 = p.map_async(zipfile_job, objects[0:150])
            first_job_1_results = list(chain(*first_job_1.get()))
            first_job_2 = p.map_async(zipfile_job, objects[150:300])
            first_job_2_results = list(chain(*first_job_2.get()))
            p.close()
            p.join()

            first_job_1_results.append(first_job_2_results)
            first_job_results = list(chain(*first_job_1_results))

            # tuple list
            second_job_list = list()
            for f in first_job_results:
                second_job = (f, s['canvas_ids'])
                second_job_list.append(second_job)

            second_job_list_chunk = list_chunk(second_job_list, 10000)
            for jobs in second_job_list_chunk:
                start_time_jobs = time.time()
                print(f'after_zipfile_job, {len(jobs)}개 / {len(second_job_list)}개 실행...')
                p = Pool(pool_num)
                second_jobs = p.starmap_async(after_zipfile_job, [s for s in jobs], callback=callback_func)
                p.close()
                p.join()
                print(f'time: {(time.time() - start_time_jobs)}초')
            print(f'{s["segment_id"]} time: {(time.time() - start_time) / 60}분')

        elif 300 < len(objects) <= 450:
            p = Pool(pool_num)
            first_job_1 = p.map_async(zipfile_job, objects[0:150])
            first_job_1_results = list(chain(*first_job_1.get()))
            first_job_2 = p.map_async(zipfile_job, objects[150:300])
            first_job_2_results = list(chain(*first_job_2.get()))
            first_job_3 = p.map_async(zipfile_job, objects[300:450])
            first_job_3_results = list(chain(*first_job_3.get()))
            p.close()
            p.join()

            first_job_1_results.append(first_job_2_results)
            first_job_1_results.append(first_job_3_results)
            first_job_results = list(chain(*first_job_1_results))

            # tuple list
            second_job_list = list()
            for f in first_job_results:
                second_job = (f, s['canvas_ids'])
                second_job_list.append(second_job)

            second_job_list_chunk = list_chunk(second_job_list, 10000)
            for jobs in second_job_list_chunk:
                start_time_jobs = time.time()
                print(f'after_zipfile_job, {len(jobs)}개 / {len(second_job_list)}개 실행...')
                p = Pool(pool_num)
                second_jobs = p.starmap_async(after_zipfile_job, [s for s in jobs], callback=callback_func)
                p.close()
                p.join()
                print(f'time: {(time.time() - start_time_jobs)}초')
            print(f'{s["segment_id"]} time: {(time.time() - start_time) / 60}분')

        elif 450 < len(objects) <= 600:
            p = Pool(pool_num)
            first_job_1 = p.map_async(zipfile_job, objects[0:150])
            first_job_1_results = list(chain(*first_job_1.get()))
            first_job_2 = p.map_async(zipfile_job, objects[150:300])
            first_job_2_results = list(chain(*first_job_2.get()))
            first_job_3 = p.map_async(zipfile_job, objects[300:450])
            first_job_3_results = list(chain(*first_job_3.get()))
            first_job_4 = p.map_async(zipfile_job, objects[450:600])
            first_job_4_results = list(chain(*first_job_4.get()))
            p.close()
            p.join()

            first_job_1_results.append(first_job_2_results)
            first_job_1_results.append(first_job_3_results)
            first_job_1_results.append(first_job_4_results)
            first_job_results = list(chain(*first_job_1_results))

            # tuple list
            second_job_list = list()
            for f in first_job_results:
                second_job = (f, s['canvas_ids'])
                second_job_list.append(second_job)

            second_job_list_chunk = list_chunk(second_job_list, 10000)
            for jobs in second_job_list_chunk:
                start_time_jobs = time.time()
                print(f'after_zipfile_job, {len(jobs)}개 / {len(second_job_list)}개 실행...')
                p = Pool(pool_num)
                second_jobs = p.starmap_async(after_zipfile_job, [s for s in jobs], callback=callback_func)
                p.close()
                p.join()
                print(f'time: {(time.time() - start_time_jobs)}초')
            print(f'{s["segment_id"]} time: {(time.time() - start_time) / 60}분')

        elif 600 < len(objects):
            print("세그먼트 사이즈를 줄여주세요.")

        else:
            print("압축파일 폴더 확인 필요")


# base_dt string, user_id string, step_id string, step_nm string, entry_time string, entry_count string, campaigns_received_ids string, target_canvas_id string, segment_id string
