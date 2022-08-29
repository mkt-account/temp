import time
import requests
import json
from datetime import datetime
from pytz import timezone
from util import get_sheet

# Braze에서 우리 S3에 기록 완료까지 수 분이 걸릴 수 있으니 넉넉하게 1시간 정도 작업으로 잡을 것
today = datetime.now(timezone('Asia/Seoul'))
today_str = today.strftime('%Y-%m-%d')


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


def requests_with_retry(r_url, r_header, r_body, interval_sec):
    s = requests.post(url=r_url, headers=r_header, json=r_body)
    if s.status_code == 429:
        print(s.status_code)
        print(f'{interval_sec}초 후 재시도...')
        time.sleep(interval_sec)
        return requests_with_retry(r_url, r_header, r_body, interval_sec)
    else:
        print(s.status_code)
        return s


segment_request_header = {
        "Content-Type": "application/json",
        "Authorization": "Bearer BRAZE-API-KEY"
    }
segment_request_url = 'https://rest.iad-03.braze.com/users/export/segment'
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
for segment in segments_info:
    segment_request_body = {
        "segment_id": segment["segment_id"],
        "fields_to_export": ["external_id", "canvases_received", "campaigns_received"],
        "output_format": "zip"
    }
    response = requests_with_retry(segment_request_url, segment_request_header, segment_request_body, 30)
    # API 요청 완료되면 S3에 기록됨(s3://bucketplace-mkt/braze/user_data/segment_export/)
    result = json.loads(response.text)
    print(segment["segment_id"])
    print(result)
