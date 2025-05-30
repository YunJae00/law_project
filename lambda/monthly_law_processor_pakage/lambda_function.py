import json
import logging
import re
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import pandas as pd
from io import StringIO

# Lambda용 로거
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class LawDataProcessorS3:
    """S3 저장용 법령 데이터 정제기"""

    def __init__(self):
        # ⚠️ S3 바뀌면 체크 필요
        self.bucket_name = ''
        self.s3_client = boto3.client('s3')
        self.success_count = 0
        self.failure_count = 0

    def clean_text(self, text):
        """텍스트 정제 함수"""
        try:
            if pd.isna(text) or text is None:
                return text

            text = str(text)

            # 불필요한 공백 정리
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'\n\s*\n', '\n', text)

            # 특수 문자 정리
            text = re.sub(r'[^\w\s\n.,;:!?()[\]{}""''「」『』【】〈〉《》\-·ㆍ/]', '', text)

            # 법령 특유의 패턴 정리
            text = re.sub(r'<[^>]+>', '', text)  # HTML 태그 제거
            text = re.sub(r'\([인印]\)', '', text)  # (인) 제거
            text = re.sub(r'⊙', '', text)  # 특수 기호 제거

            return text.strip()

        except Exception as e:
            logger.error(f"텍스트 정제 오류: {e}")
            return text

    def clean_revision_reason(self, text):
        """제개정이유 특별 정제 함수"""
        try:
            if pd.isna(text):
                return text

            text = self.clean_text(text)

            # 기호 정리
            text = re.sub(r'[◇○◆●]', '-', text)
            text = text.replace('<법제처 제공>', '')

            return text.strip()

        except Exception as e:
            logger.error(f"제개정이유 정제 오류: {e}")
            return text

    def parse_date(self, date_str):
        """날짜 문자열을 datetime 객체로 변환"""
        try:
            if pd.isna(date_str) or not date_str:
                return None

            date_str = str(date_str).strip()
            date_formats = ['%Y%m%d', '%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d']

            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            return None

        except Exception as e:
            logger.error(f"날짜 파싱 오류: {e}")
            return None

    def calculate_grace_period(self, promulgation_date, enforcement_date):
        """공포일자와 시행일자 간의 유예기간 계산"""
        try:
            if promulgation_date and enforcement_date:
                return (enforcement_date - promulgation_date).days
            return None
        except Exception as e:
            logger.error(f"유예기간 계산 오류: {e}")
            return None

    def load_s3_file(self, s3_key):
        """S3에서 JSON 파일 로드"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            return data.get('raw_data')
        except Exception as e:
            logger.error(f"S3 파일 로드 오류 ({s3_key}): {e}")
            return None

    def process_to_dataframe(self, raw_data):
        """raw 데이터를 pandas DataFrame으로 변환"""
        try:
            if not raw_data.get('법령'):
                return pd.DataFrame()

            root_data = raw_data['법령']
            record = {}

            # 법령키
            if root_data.get('법령키'):
                record['법령키'] = root_data['법령키']

            # 기본정보 추출
            if root_data.get('기본정보'):
                basic_info = root_data['기본정보']
                record.update({
                    '법령명_한글': basic_info.get('법령명_한글', ''),
                    '법령명_한자': basic_info.get('법령명_한자', ''),
                    '법령명약칭': basic_info.get('법령명약칭', ''),
                    '공포번호': basic_info.get('공포번호', ''),
                    '공포일자': basic_info.get('공포일자', ''),
                    '시행일자': basic_info.get('시행일자', ''),
                    '제개정구분': basic_info.get('제개정구분', ''),
                    '법령ID': basic_info.get('법령ID', ''),
                    '언어': basic_info.get('언어', ''),
                    '전화번호': basic_info.get('전화번호', ''),
                    '편장절관': basic_info.get('편장절관', ''),
                    '공포법령여부': basic_info.get('공포법령여부', ''),
                })

                # 날짜 처리 및 유예기간 계산
                promulgation_date = self.parse_date(basic_info.get('공포일자'))
                enforcement_date = self.parse_date(basic_info.get('시행일자'))

                record['공포일자_datetime'] = promulgation_date
                record['시행일자_datetime'] = enforcement_date
                record['유예기간_일수'] = self.calculate_grace_period(promulgation_date, enforcement_date)

                # 소관부처 정보
                if basic_info.get('소관부처'):
                    record['소관부처'] = basic_info['소관부처'].get('content', '')
                    record['소관부처코드'] = basic_info['소관부처'].get('소관부처코드', '')

                # 법종구분 정보
                if basic_info.get('법종구분'):
                    record['법종구분'] = basic_info['법종구분'].get('content', '')
                    record['법종구분코드'] = basic_info['법종구분'].get('법종구분코드', '')

            # 개정문 내용 추출
            if root_data.get('개정문') and root_data['개정문'].get('개정문내용'):
                revision_contents = root_data['개정문']['개정문내용']
                all_revision_text = []
                for content_list in revision_contents:
                    if isinstance(content_list, list):
                        all_revision_text.extend(content_list)
                    else:
                        all_revision_text.append(str(content_list))
                record['개정문내용'] = self.clean_text('\n'.join(all_revision_text))

            # 조문 정보 통계
            if root_data.get('조문') and root_data['조문'].get('조문단위'):
                record['총_조문수'] = len(root_data['조문']['조문단위'])

            # 부칙 정보 통계
            if root_data.get('부칙') and root_data['부칙'].get('부칙단위'):
                record['총_부칙수'] = len(root_data['부칙']['부칙단위'])

            # 제개정이유 추출
            if root_data.get('제개정이유') and root_data['제개정이유'].get('제개정이유내용'):
                reason_contents = root_data['제개정이유']['제개정이유내용']
                reason_text_list = []
                for reason_group in reason_contents:
                    if isinstance(reason_group, list):
                        reason_text_list.extend(reason_group)
                    else:
                        reason_text_list.append(str(reason_group))
                record['제개정이유내용'] = self.clean_revision_reason('\n'.join(reason_text_list))

            return pd.DataFrame([record])

        except Exception as e:
            logger.error(f"DataFrame 변환 오류: {e}")
            return pd.DataFrame()

    def get_s3_content_files(self, month_str):
        """S3에서 해당 월의 본문 파일들 가져오기"""
        prefix = f'eflaw_contents/{month_str}/'

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )

            if 'Contents' not in response:
                logger.warning(f"S3에서 {prefix} 경로에 파일이 없습니다.")
                return []

            s3_keys = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json')]
            logger.info(f"본문 파일 {len(s3_keys)}개 발견")
            return s3_keys

        except ClientError as e:
            logger.error(f"S3 파일 목록 조회 실패: {e}")
            return []

    def save_csv_to_s3(self, df, month_str):
        """DataFrame을 CSV로 변환하여 S3에 저장"""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()

            s3_key = f'processed_data/{month_str}/{month_str}_processed_eflaw_data.csv'

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv'
            )

            logger.info(f'CSV 파일 S3 저장 완료: {s3_key}')
            return s3_key

        except Exception as e:
            logger.error(f'CSV S3 저장 실패: {e}')
            return None

    def print_summary_stats(self, df, month_str):
        """결과 요약 통계 출력"""
        try:
            logger.info(f"{month_str} 처리 완료 - 전체 법령 수: {len(df)}")

            # 법종구분별 분포 (상위 5개)
            if '법종구분' in df.columns:
                law_type_counts = df['법종구분'].value_counts().head(5)
                logger.info(f"주요 법종구분: {dict(law_type_counts)}")

            # 소관부처별 분포 (상위 5개)
            if '소관부처' in df.columns:
                dept_counts = df['소관부처'].value_counts().head(5)
                logger.info(f"주요 소관부처: {dict(dept_counts)}")

            # 유예기간 통계
            if '유예기간_일수' in df.columns and not df['유예기간_일수'].isna().all():
                grace_stats = df['유예기간_일수'].describe()
                logger.info(f"유예기간 통계 - 평균: {grace_stats['mean']:.1f}일, 최대: {grace_stats['max']:.0f}일")

        except Exception as e:
            logger.error(f"통계 출력 오류: {e}")

    def process_s3_data(self, month_str):
        """S3의 본문 데이터를 정제하여 CSV로 저장"""
        try:
            logger.info(f'{month_str} 데이터 정제 시작')

            # S3에서 본문 파일들 가져오기
            content_files = self.get_s3_content_files(month_str)
            if not content_files:
                return {
                    'statusCode': 200,
                    'body': {
                        'message': f'{month_str}에 처리할 파일이 없습니다.',
                        'csv_file': None
                    }
                }

            logger.info(f'총 {len(content_files)}개 파일 정제 시작')

            df_list = []

            # 파일 정제
            for i, s3_key in enumerate(content_files):
                try:
                    raw_data = self.load_s3_file(s3_key)
                    if raw_data:
                        processed_df = self.process_to_dataframe(raw_data)
                        if not processed_df.empty:
                            df_list.append(processed_df)
                        self.success_count += 1
                    else:
                        self.failure_count += 1

                    # 진행률 출력 (100개 파일마다)
                    if (i + 1) % 100 == 0:
                        progress = ((i + 1) / len(content_files)) * 100
                        logger.info(f'진행률: {progress:.1f}% ({i + 1}/{len(content_files)})')

                except Exception as e:
                    self.failure_count += 1
                    logger.error(f'파일 처리 오류 ({s3_key}): {e}')

            if df_list:
                # 모든 DataFrame 합치기
                df_merged = pd.concat(df_list, ignore_index=True)

                # CSV로 S3에 저장
                csv_s3_key = self.save_csv_to_s3(df_merged, month_str)

                # 결과 요약 통계 출력
                self.print_summary_stats(df_merged, month_str)

                logger.info(f"{month_str} 정제 완료 - 성공: {self.success_count}, 실패: {self.failure_count}")

                return {
                    'statusCode': 200,
                    'body': {
                        'message': f'{month_str} 데이터 정제 완료',
                        'total_files': len(content_files),
                        'success_count': self.success_count,
                        'failure_count': self.failure_count,
                        'total_records': len(df_merged),
                        'csv_file': csv_s3_key,
                        'bucket': self.bucket_name
                    }
                }
            else:
                return {
                    'statusCode': 200,
                    'body': {
                        'message': f'{month_str} 처리된 데이터가 없습니다.',
                        'csv_file': None
                    }
                }

        except Exception as e:
            logger.error(f'{month_str} 처리 과정 오류: {e}')
            return {
                'statusCode': 500,
                'body': {'error': f'데이터 처리 실패: {str(e)}'}
            }


def lambda_handler(event, context):
    """Lambda 핸들러 함수"""
    try:
        logger.info(f"법령 데이터 정제 시작 - event: {event}")

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

        processor = LawDataProcessorS3()

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

        # 데이터 정제 실행
        result = processor.process_s3_data(month_str)

        logger.info(f"법령 데이터 정제 완료")
        return result

    except Exception as e:
        logger.error(f'Lambda 실행 오류: {e}')
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }
