import calendar
import datetime
import json
import logging
import time
import boto3
from botocore.exceptions import ClientError
import requests

# Lambda용 로거
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LawListCollectorS3:
    """S3 저장용 법령 목록 수집기"""

    def __init__(self):
        # ⚠️ 나중에 API 키 바뀌면 체크 필요
        self.api_key = ''
        self.base_url = 'https://www.law.go.kr'
        self.display = 100
        # ⚠️ 나중에 s3 바뀌면 체크 필요
        self.bucket_name = ''

        self.s3_client = boto3.client('s3')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_month_date_range(self, year, month):
        """년/월의 시작일과 종료일 반환"""
        start_date = datetime.date(year, month, 1)
        _, last_day = calendar.monthrange(year, month)
        end_date = datetime.date(year, month, last_day)
        return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')

    def get_total_count(self, year, month):
        """해당 월의 총 데이터 개수 조회"""
        start_date, end_date = self.get_month_date_range(year, month)

        params = {
            'OC': self.api_key,
            'target': 'eflaw',
            'page': '1',
            'display': '1',
            'type': 'JSON',
            'efYd': f'{start_date}~{end_date}',
        }

        try:
            url = f'{self.base_url}/DRF/lawSearch.do'
            response = self.session.get(url, params=params, timeout=30)

            if response.status_code == 200:
                data = response.json()
                return int(data['LawSearch']['totalCnt'])
            else:
                logger.warning(f'총 데이터 수 조회 실패: HTTP {response.status_code}')
                return 0

        except Exception as e:
            logger.error(f'총 데이터 수 조회 오류: {e}')
            return 0

    def collect_page(self, year, month, page):
        """특정 페이지 데이터 수집"""
        start_date, end_date = self.get_month_date_range(year, month)

        params = {
            'OC': self.api_key,
            'target': 'eflaw',
            'page': page,
            'display': self.display,
            'efYd': f'{start_date}~{end_date}',
            'type': 'JSON'
        }

        try:
            url = f'{self.base_url}/DRF/lawSearch.do'
            response = self.session.get(url, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f'페이지 {page} 데이터 조회 실패: HTTP {response.status_code}')
                return None

        except Exception as e:
            logger.error(f'페이지 {page} 조회 오류: {e}')
            return None

    def save_to_s3(self, data, year, month, page):
        """JSON 데이터를 S3에 저장"""
        timestamp = datetime.datetime.now()
        month_str = f"{year}-{month:02d}"

        raw_record = {
            "metadata": {
                "timestamp": timestamp.isoformat(),
                "period": month_str,
                "page": page,
                "year": year,
                "month": month
            },
            "raw_data": data
        }

        s3_key = f'eflaw_lists/{month_str}/{month_str}_page{page}.json'

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

    def collect_month_data(self, year, month):
        """특정 년/월의 법령 목록 수집"""
        month_str = f"{year}-{month:02d}"
        logger.info(f'{month_str} 법령 목록 수집 시작')

        # 총 개수 조회
        total_count = self.get_total_count(year, month)
        if total_count == 0:
            logger.info(f'{month_str}에 데이터가 없습니다.')
            return {
                'statusCode': 200,
                'body': {
                    'message': f'{month_str}에 데이터가 없습니다.',
                    'total_count': 0,
                    'saved_files': []
                }
            }

        total_pages = (total_count // self.display) + 1
        logger.info(f'총 {total_count}개 데이터, {total_pages}개 페이지 수집 시작')

        saved_files = []

        # 페이지별 데이터 수집
        for page in range(1, total_pages + 1):
            data = self.collect_page(year, month, page)
            if data:
                s3_key = self.save_to_s3(data, year, month, page)
                if s3_key:
                    saved_files.append(s3_key)

            # API 호출 간격 조정
            if page < total_pages:
                time.sleep(0.1)

        logger.info(f'{month_str} 수집 완료: {len(saved_files)}/{total_pages} 파일 저장')

        return {
            'statusCode': 200,
            'body': {
                'message': f'{month_str} 데이터 수집 완료',
                'total_count': total_count,
                'total_pages': total_pages,
                'saved_files': saved_files,
                'bucket': self.bucket_name
            }
        }


def lambda_handler(event, context):
    """Lambda 핸들러 함수"""
    try:
        logger.info(f"법령 목록 수집 시작 - event: {event}")

        # event 파싱
        # dag에서 payload로 온 json 파싱
        if isinstance(event, str):
            try:
                event = json.loads(event)
            except json.JSONDecodeError as e:
                logger.error(f"Event 파싱 실패: {e}")
                return {
                    'statusCode': 400,
                    'body': {'error': f'Event 파싱 실패: {str(e)}'}
                }

        collector = LawListCollectorS3()

        # 파라미터 추출
        year = event.get('year') if isinstance(event, dict) else None
        month = event.get('month') if isinstance(event, dict) else None

        if year and month:
            # 지정된 년월 수집
            result = collector.collect_month_data(int(year), int(month))
        else:
            # 다음달 수집 (기본값)
            today = datetime.datetime.now()
            next_month = today + datetime.timedelta(days=32)
            next_month = next_month.replace(day=1)
            result = collector.collect_month_data(next_month.year, next_month.month)

        logger.info(f"법령 목록 수집 완료")
        return result

    except Exception as e:
        logger.error(f'Lambda 실행 오류: {e}')
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }

