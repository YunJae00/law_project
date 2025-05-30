import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError
import requests
from urllib.parse import urljoin
from datetime import datetime

# Lambda용 로거
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LawContentCollectorS3:
    """S3 저장용 법령 본문 수집기"""

    def __init__(self):
        # ⚠️ 나중에 key 바뀌면 체크 필요
        self.api_key = ''
        self.base_url = 'https://www.law.go.kr'
        # ⚠️ 나중에 s3 바뀌면 체크 필요
        self.bucket_name = ''

        self.s3_client = boto3.client('s3')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        self.success_count = 0
        self.error_count = 0
        self.lock = threading.Lock()

    def update_counts(self, is_success=True):
        """카운터 업데이트"""
        with self.lock:
            if is_success:
                self.success_count += 1
            else:
                self.error_count += 1

    def get_law_content(self, law_id, mst, detail_link):
        """상세 링크로 본문 가져오기"""
        if detail_link.startswith('/'):
            full_url = urljoin(self.base_url, detail_link)
        else:
            full_url = detail_link

        full_url = full_url.replace('type=HTML', 'type=JSON')

        try:
            response = self.session.get(full_url, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"HTTP {response.status_code} for {law_id}")
                return None
        except Exception as e:
            logger.error(f"본문 수집 오류 ({law_id}): {e}")
            return None

    def save_content_to_s3(self, content_data, law_id, mst, month_str):
        """본문 데이터를 S3에 저장"""
        timestamp = datetime.now()

        raw_record = {
            "metadata": {
                "timestamp": timestamp.isoformat(),
                "law_id": law_id,
                "mst": mst,
                "month": month_str
            },
            "raw_data": content_data
        }

        s3_key = f'eflaw_contents/{month_str}/eflaw_content_{law_id}_{mst}.json'

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(raw_record, ensure_ascii=False, indent=2),
                ContentType='application/json'
            )
            return s3_key
        except ClientError as e:
            logger.error(f'S3 저장 실패: {e}')
            return None

    def process_single_law(self, law_data, month_str):
        """개별 법령 본문 수집 처리"""
        law_id = law_data.get('법령ID')
        mst = law_data.get('법령일련번호')
        detail_link = law_data.get('법령상세링크')
        law_name = law_data.get('법령명한글', 'Unknown')

        if not detail_link or not law_id:
            self.update_counts(is_success=False)
            return None

        try:
            # 법령 본문 가져오기
            content_data = self.get_law_content(law_id, mst, detail_link)

            if content_data:
                s3_key = self.save_content_to_s3(content_data, law_id, mst, month_str)
                if s3_key:
                    self.update_counts(is_success=True)
                    return s3_key
                else:
                    self.update_counts(is_success=False)
                    return None
            else:
                self.update_counts(is_success=False)
                return None

        except Exception as e:
            logger.error(f'개별 법령 처리 오류 ({law_name}): {e}')
            self.update_counts(is_success=False)
            return None

    def get_s3_list_files(self, month_str):
        """S3에서 해당 월의 리스트 파일들 가져오기"""
        prefix = f'eflaw_lists/{month_str}/'

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Contents' not in response:
                logger.warning(f"S3에서 {prefix} 경로에 파일이 없습니다.")
                return []

            s3_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
            logger.info(f"리스트 파일 {len(s3_keys)}개 발견")
            return s3_keys

        except ClientError as e:
            logger.error(f"S3 파일 목록 조회 실패: {e}")
            return []

    def load_s3_file(self, s3_key):
        """S3에서 파일 로드"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            logger.error(f"S3 파일 로드 실패 ({s3_key}): {e}")
            return None

    def collect_from_s3_lists(self, month_str, workers=3):
        """S3의 리스트 파일들로부터 본문 수집"""
        logger.info(f'{month_str} 법령 본문 수집 시작')

        # S3에서 리스트 파일들 가져오기
        list_files = self.get_s3_list_files(month_str)
        if not list_files:
            return {
                'statusCode': 200,
                'body': {
                    'message': f'{month_str}에 리스트 파일이 없습니다.',
                    'collected_files': []
                }
            }

        # 각 리스트 파일에서 법령 데이터 추출
        all_law_data = []
        for s3_key in list_files:
            raw_data = self.load_s3_file(s3_key)
            if not raw_data:
                continue

            try:
                if 'raw_data' in raw_data and 'LawSearch' in raw_data['raw_data']:
                    law_search = raw_data['raw_data']['LawSearch']
                    if 'law' in law_search and law_search['law']:
                        all_law_data.extend(law_search['law'])
            except Exception as e:
                logger.error(f'파일 파싱 오류 ({s3_key}): {e}')

        if not all_law_data:
            return {
                'statusCode': 200,
                'body': {
                    'message': f'{month_str}에 처리할 법령 데이터가 없습니다.',
                    'collected_files': []
                }
            }

        logger.info(f'총 {len(all_law_data)}개 법령 본문 수집 시작')

        collected_files = []

        # ThreadPoolExecutor로 병렬 처리
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_law = {
                executor.submit(self.process_single_law, law_data, month_str): law_data
                for law_data in all_law_data
            }

            for future in as_completed(future_to_law):
                try:
                    result = future.result(timeout=60)
                    if result:
                        collected_files.append(result)
                except Exception as e:
                    law_data = future_to_law[future]
                    logger.error(f"작업 실행 오류 ({law_data.get('법령명한글', 'Unknown')}): {e}")

                time.sleep(0.1)

        logger.info(f'{month_str} 본문 수집 완료: {len(collected_files)}/{len(all_law_data)} 성공')

        return {
            'statusCode': 200,
            'body': {
                'message': f'{month_str} 법령 본문 수집 완료',
                'total_laws': len(all_law_data),
                'success_count': self.success_count,
                'error_count': self.error_count,
                'collected_files': collected_files,
                'bucket': self.bucket_name
            }
        }


def lambda_handler(event, context):
    """Lambda 핸들러 함수"""
    try:
        logger.info(f"법령 본문 수집 시작 - event: {event}")

        # event 파싱
        if isinstance(event, str):
            try:
                event = json.loads(event)
            except json.JSONDecodeError as e:
                logger.error(f"Event 파싱 실패: {e}")
                return {
                    'statusCode': 400,
                    'body': {'error': f'Event 파싱 실패: {str(e)}'}
                }

        collector = LawContentCollectorS3()

        # 파라미터 추출
        year = event.get('year') if isinstance(event, dict) else None
        month = event.get('month') if isinstance(event, dict) else None
        month_str = event.get('month_str') if isinstance(event, dict) else None

        # month_str이 없으면 year, month로 생성
        if not month_str and year and month:
            month_str = f"{year}-{month:02d}"

        if not month_str:
            error_msg = "year, month 또는 month_str 파라미터가 필요합니다."
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': {'error': error_msg}
            }

        # 본문 수집
        result = collector.collect_from_s3_lists(month_str, workers=3)

        logger.info(f"법령 본문 수집 완료")
        return result

    except Exception as e:
        logger.error(f'Lambda 실행 오류: {e}')
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

