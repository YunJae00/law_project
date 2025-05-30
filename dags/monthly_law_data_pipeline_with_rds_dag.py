from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import json
import logging

# 기본 인수 설정
default_args = {
    'owner': 'law-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# 공통 함수
# =============================================================================

def _parse_lambda_response(lambda_result):
    """Lambda 응답 파싱 공통 함수"""
    try:
        if isinstance(lambda_result, dict):
            if 'Payload' in lambda_result:
                payload_raw = lambda_result['Payload']
                if hasattr(payload_raw, 'read'):
                    payload_str = payload_raw.read().decode('utf-8')
                    return json.loads(payload_str)
                elif isinstance(payload_raw, str):
                    return json.loads(payload_raw)
                else:
                    return payload_raw
            else:
                return lambda_result
        elif isinstance(lambda_result, str):
            return json.loads(lambda_result)
        else:
            logging.error(f"예상치 못한 Lambda 결과 타입: {type(lambda_result)}")
            return None
    except json.JSONDecodeError as e:
        logging.error(f"JSON 파싱 실패: {e}")
        return None
    except Exception as e:
        logging.error(f"응답 파싱 중 오류: {e}")
        return None

# =============================================================================
# 단계별 함수 정의
# =============================================================================

def prepare_pipeline_payload(**context):
    """파이프라인 실행을 위한 페이로드 준비"""
    dag_run = context['dag_run']
    conf = dag_run.conf or {}

    year = conf.get('year') or context['params'].get('year')
    month = conf.get('month') or context['params'].get('month')

    if not year or not month:
        now = datetime.now()
        year = now.year
        month = now.month

    month_str = f"{int(year)}-{int(month):02d}"

    payload = {
        'year': int(year),
        'month': int(month),
        'month_str': month_str
    }

    logging.info(f"법령 데이터 파이프라인 시작: {payload}")
    # XCom에 자동 저장
    # airflow 메타데이터베이스의 xcom 테이블에 insert 해서 나중에 계속 가져옴
    return payload

# ========== 1단계: 법령 목록 수집 ==========

def process_list_collection_result(**context):
    """법령 목록 수집 결과 처리"""
    try:
        # 이전 태스크의 결과를 받아서 처리
        lambda_result = context['task_instance'].xcom_pull(task_ids='collect_law_lists')
        logging.info(lambda_result)

        if lambda_result is None:
            raise Exception("법령 목록 수집 Lambda 결과가 None입니다.")

        payload = _parse_lambda_response(lambda_result)
        if not payload or payload.get('statusCode') != 200:
            raise Exception(f"법령 목록 수집 실패: {payload}")

        body = payload.get('body', {})
        total_count = body.get('total_count', 0)
        saved_files = body.get('saved_files', [])

        logging.info(f"법령 목록 수집 완료 - 총 데이터: {total_count}개, 파일: {len(saved_files)}개")

        return {
            'success': True,
            'total_count': total_count,
            'files_count': len(saved_files)
        }

    except Exception as e:
        error_msg = f"법령 목록 수집 결과 처리 오류: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

# ========== 2단계: 법령 본문 수집 ==========

def invoke_content_collection_with_timeout(**context):
    """법령 본문 수집 Lambda 호출 (타임아웃 설정)"""
    payload = context['task_instance'].xcom_pull(task_ids='prepare_pipeline_payload')

    lambda_hook = LambdaHook(
        aws_conn_id='aws_default',
        config={
            'read_timeout': 900,  # 15분
            'connect_timeout': 60  # 1분
        }
    )

    response = lambda_hook.invoke_lambda(
        # ⚠️ 나중에 혹시 Lambda 함수 바뀌면 체크 필요
        function_name='',
        payload=json.dumps(payload),
        invocation_type='RequestResponse'
    )

    """
    response = {
        'ResponseMetadata': {
            'RequestId': '',  # AWS 요청 ID
            'HTTPStatusCode': 200,     # HTTP 상태 코드
            'HTTPHeaders': {...},      # HTTP 헤더들
            'RetryAttempts': 0         # 재시도 횟수
        },
        'StatusCode': 200,             # Lambda 호출 상태 코드
        'ExecutedVersion': '$LATEST',   # 실행된 Lambda 버전
        'Payload': <StreamingBody object>  # 실제 Lambda 함수 결과 -> 이거 뽑아서 씀
    }
    """

    # StreamingBody 처리
    if 'Payload' in response:
        payload_data = response['Payload'].read().decode('utf-8')
        try:
            parsed_payload = json.loads(payload_data)
            response['Payload'] = parsed_payload
        except json.JSONDecodeError:
            response['Payload'] = payload_data

    logging.info(f"법령 본문 수집 Lambda 호출 완료: StatusCode={response.get('StatusCode')}")
    return response

def process_content_collection_result(**context):
    """법령 본문 수집 결과 처리"""
    try:
        lambda_result = context['task_instance'].xcom_pull(task_ids='collect_law_contents')

        if lambda_result is None:
            raise Exception("법령 본문 수집 Lambda 결과가 None입니다.")

        payload = _parse_lambda_response(lambda_result)
        if not payload or payload.get('statusCode') != 200:
            raise Exception(f"법령 본문 수집 실패: {payload}")

        body = payload.get('body', {})
        total_laws = body.get('total_laws', 0)
        collected_files = body.get('collected_files', [])
        success_count = body.get('success_count', 0)
        error_count = body.get('error_count', 0)

        logging.info(f"법령 본문 수집 완료 - 총 법령: {total_laws}개, 수집: {len(collected_files)}개, 성공/실패: {success_count}/{error_count}")

        return {
            'success': True,
            'total_laws': total_laws,
            'collected_files': len(collected_files),
            'success_count': success_count,
            'error_count': error_count
        }

    except Exception as e:
        error_msg = f"법령 본문 수집 결과 처리 오류: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

# ========== 3단계: 데이터 정제 ==========

def invoke_data_processing_with_timeout(**context):
    """데이터 정제 Lambda 호출 (타임아웃 설정)"""
    payload = context['task_instance'].xcom_pull(task_ids='prepare_pipeline_payload')

    lambda_hook = LambdaHook(
        aws_conn_id='aws_default',
        config={
            'read_timeout': 900,  # 15분
            'connect_timeout': 60  # 1분
        }
    )

    response = lambda_hook.invoke_lambda(
        # ⚠️ 나중에 혹시 Lambda 함수 바뀌면 체크 필요
        function_name='monthly-law-processor',
        payload=json.dumps(payload),
        invocation_type='RequestResponse'
    )

    # StreamingBody 처리
    if 'Payload' in response:
        payload_data = response['Payload'].read().decode('utf-8')
        try:
            parsed_payload = json.loads(payload_data)
            response['Payload'] = parsed_payload
        except json.JSONDecodeError:
            response['Payload'] = payload_data

    logging.info(f"데이터 정제 Lambda 호출 완료: StatusCode={response.get('StatusCode')}")
    return response

def process_data_processing_result(**context):
    """데이터 정제 결과 처리"""
    try:
        lambda_result = context['task_instance'].xcom_pull(task_ids='process_law_data')

        if lambda_result is None:
            raise Exception("데이터 정제 Lambda 결과가 None입니다.")

        payload = _parse_lambda_response(lambda_result)
        if not payload or payload.get('statusCode') != 200:
            raise Exception(f"데이터 정제 실패: {payload}")

        body = payload.get('body', {})
        total_files = body.get('total_files', 0)
        total_records = body.get('total_records', 0)
        success_count = body.get('success_count', 0)
        failure_count = body.get('failure_count', 0)
        csv_file = body.get('csv_file', 'None')

        logging.info(f"데이터 정제 완료 - 파일: {total_files}개, 레코드: {total_records}개, CSV: {csv_file}")

        return {
            'success': True,
            'total_files': total_files,
            'total_records': total_records,
            'success_count': success_count,
            'failure_count': failure_count,
            'csv_file': csv_file
        }

    except Exception as e:
        error_msg = f"데이터 정제 결과 처리 오류: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

# ========== 4단계: RDS 직접 저장 ==========

def invoke_rds_loader_with_timeout(**context):
    """RDS 직접 저장 Lambda 호출 (타임아웃 설정)"""
    payload = context['task_instance'].xcom_pull(task_ids='prepare_pipeline_payload')
    processing_result = context['task_instance'].xcom_pull(task_ids='process_processing_result')

    # CSV 파일 경로 및 추가 옵션 설정
    payload['csv_s3_key'] = processing_result.get('csv_file')
    payload['replace_existing'] = True

    lambda_hook = LambdaHook(
        aws_conn_id='aws_default',
        config={
            'read_timeout': 900,  # 15분
            'connect_timeout': 60  # 1분
        }
    )

    response = lambda_hook.invoke_lambda(
        # ⚠️ 나중에 혹시 Lambda 함수 바뀌면 체크 필요
        function_name='',
        payload=json.dumps(payload),
        invocation_type='RequestResponse'
    )

    # StreamingBody 처리
    if 'Payload' in response:
        payload_data = response['Payload'].read().decode('utf-8')
        try:
            parsed_payload = json.loads(payload_data)
            response['Payload'] = parsed_payload
        except json.JSONDecodeError:
            response['Payload'] = payload_data

    logging.info(f"RDS 직접 저장 Lambda 호출 완료: StatusCode={response.get('StatusCode')}")
    return response

def process_rds_loader_result(**context):
    """RDS 직접 저장 결과 처리"""
    try:
        lambda_result = context['task_instance'].xcom_pull(task_ids='load_to_rds')

        if lambda_result is None:
            raise Exception("RDS 직접 저장 Lambda 결과가 None입니다.")

        payload = _parse_lambda_response(lambda_result)
        if not payload or payload.get('statusCode') != 200:
            raise Exception(f"RDS 직접 저장 실패: {payload}")

        body = payload.get('body', {})
        deleted_count = body.get('deleted_count', 0)
        inserted_count = body.get('inserted_count', 0)
        success_count = body.get('success_count', 0)
        error_count = body.get('error_count', 0)
        duration_seconds = body.get('duration_seconds', 0)

        logging.info(f"RDS 직접 저장 완료 - 삭제: {deleted_count}건, 삽입: {inserted_count}건, 소요시간: {duration_seconds}초")

        return {
            'success': True,
            'deleted_count': deleted_count,
            'inserted_count': inserted_count,
            'success_count': success_count,
            'error_count': error_count,
            'duration_seconds': duration_seconds,
        }

    except Exception as e:
        error_msg = f"RDS 직접 저장 결과 처리 오류: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

# ========== 최종 단계: 파이프라인 완료 ==========

def finalize_rds_pipeline(**context):
    """RDS 직접 저장 파이프라인 완료 및 최종 결과 출력"""
    try:
        # 각 단계 결과 수집
        list_result = context['task_instance'].xcom_pull(task_ids='process_list_result')
        content_result = context['task_instance'].xcom_pull(task_ids='process_content_result')
        processing_result = context['task_instance'].xcom_pull(task_ids='process_processing_result')
        rds_result = context['task_instance'].xcom_pull(task_ids='process_rds_result')

        # 파이프라인 정보 수집
        pipeline_payload = context['task_instance'].xcom_pull(task_ids='prepare_pipeline_payload')
        month_str = pipeline_payload.get('month_str', 'Unknown')

        # 최종 결과
        logging.info("=" * 70)
        logging.info(f"1단계 - 목록 수집: {list_result.get('total_count', 0)}개 데이터")
        logging.info(f"2단계 - 본문 수집: {content_result.get('total_laws', 0)}개 법령")
        logging.info(f"3단계 - 데이터 정제: {processing_result.get('total_records', 0)}개 레코드")
        logging.info(f"4단계 - RDS 저장: {rds_result.get('inserted_count', 0)}개 저장")
        logging.info(f"CSV 파일: {processing_result.get('csv_file', 'None')}")
        logging.info("=" * 70)

        return {
            'pipeline_success': True,
            'month_str': month_str,
            'summary': {
                'collected': list_result.get('total_count', 0),
                'processed': processing_result.get('total_records', 0),
                'saved_to_rds': rds_result.get('inserted_count', 0),
                'csv_file': processing_result.get('csv_file'),
                'rds_duration': rds_result.get('duration_seconds', 0),
            }
        }

    except Exception as e:
        error_msg = f"RDS 파이프라인 완료 처리 오류: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

# =============================================================================
# DAG 정의
# =============================================================================

rds_law_pipeline_dag = DAG(
    'law_data_pipeline_with_rds_unified',
    default_args=default_args,
    description='완전한 법령 데이터 파이프라인 - 목록수집→본문수집→데이터정제→RDS직접저장',
    schedule_interval=None,  # 수동 실행만
    catchup=False,
    params={'year': 2024, 'month': 12}, # 이게 있어야 처음 시작할 때 입력받음
    tags=['law', 'pipeline', 'rds', 'complete', 'etl'],
    max_active_runs=1
)

# =============================================================================
# 태스크 정의
# 각 태스크 별로 context 생김
# 각 태스크 별로 return 하는게 xcom 에서 저장되어 나중에 context['task_instance'].xcom_pull(task_ids='')
# 위와 같은 방식으로 사용
# =============================================================================

# 시작 태스크
start_rds_pipeline = DummyOperator(
    task_id='start_rds_pipeline',
    dag=rds_law_pipeline_dag
)

# 0단계: 파이프라인 준비
# Lambda 함수들이 일관된 형태의 데이터를 받도록 보장하기 위해 day_run 객체의 conf를 사용하지 않음
prepare_pipeline_payload_task = PythonOperator(
    task_id='prepare_pipeline_payload',
    python_callable=prepare_pipeline_payload,
    dag=rds_law_pipeline_dag
)

# 1단계: 법령 목록 수집
# Lambda 로 전달되는 데이터 -> payload 에서 뽑아서
# { "year": 2024, "month": 12, "month_str": "2024-12" } 이런식으로 json 으로 event로 가서 처리
collect_law_lists_task = LambdaInvokeFunctionOperator(
    task_id='collect_law_lists',
    # ⚠️ 나중에 혹시 Lambda 함수 바뀌면 체크 필요
    function_name='',
    # LambdaInvokeFunctionOperator 는 dag 정의 시점에 생성되는데 아직 태스크가 실행되지 않아서
    # 태스크 실행 전에 payload=payload 하면 아무것도 안가서 template 적용 -> 특정 태스크의 XCom 데이터를 가져옴
    payload="{{ task_instance.xcom_pull(task_ids='prepare_pipeline_payload') | tojson }}",
    aws_conn_id='aws_default',
    dag=rds_law_pipeline_dag
)

process_list_result_task = PythonOperator(
    task_id='process_list_result',
    python_callable=process_list_collection_result,
    dag=rds_law_pipeline_dag
)

# 2단계: 법령 본문 수집
collect_law_contents_task = PythonOperator(
    task_id='collect_law_contents',
    python_callable=invoke_content_collection_with_timeout,
    dag=rds_law_pipeline_dag
)

process_content_result_task = PythonOperator(
    task_id='process_content_result',
    python_callable=process_content_collection_result,
    dag=rds_law_pipeline_dag
)

# 3단계: 데이터 정제
process_law_data_task = PythonOperator(
    task_id='process_law_data',
    python_callable=invoke_data_processing_with_timeout,
    dag=rds_law_pipeline_dag
)

process_processing_result_task = PythonOperator(
    task_id='process_processing_result',
    python_callable=process_data_processing_result,
    dag=rds_law_pipeline_dag
)

# 4단계: RDS 직접 저장
load_to_rds_task = PythonOperator(
    task_id='load_to_rds',
    python_callable=invoke_rds_loader_with_timeout,
    dag=rds_law_pipeline_dag
)

process_rds_result_task = PythonOperator(
    task_id='process_rds_result',
    python_callable=process_rds_loader_result,
    dag=rds_law_pipeline_dag
)

# 최종 단계: RDS 파이프라인 완료
finalize_rds_pipeline_task = PythonOperator(
    task_id='finalize_rds_pipeline',
    python_callable=finalize_rds_pipeline,
    dag=rds_law_pipeline_dag
)

# 완료 태스크
end_rds_pipeline = DummyOperator(
    task_id='end_rds_pipeline',
    dag=rds_law_pipeline_dag
)

# =============================================================================
# 태스크 의존성 설정
# =============================================================================

start_rds_pipeline >> prepare_pipeline_payload_task

# 1단계: 법령 목록 수집
prepare_pipeline_payload_task >> collect_law_lists_task >> process_list_result_task

# 2단계: 법령 본문 수집 (목록 수집 완료 후)
process_list_result_task >> collect_law_contents_task >> process_content_result_task

# 3단계: 데이터 정제 (본문 수집 완료 후)
process_content_result_task >> process_law_data_task >> process_processing_result_task

# 4단계: RDS 직접 저장 (데이터 정제 완료 후)
process_processing_result_task >> load_to_rds_task >> process_rds_result_task

# 최종 단계
process_rds_result_task >> finalize_rds_pipeline_task >> end_rds_pipeline

# =============================================================================
# DAG 문서화
# =============================================================================

rds_law_pipeline_dag.doc_md = """
# 법령 데이터 파이프라인

## 목적
지정된 년/월의 법령 데이터를 **목록수집 → 본문수집 → 데이터정제 → RDS직접저장** 순서로 처리하는 완전한 ETL 파이프라인

## 사용법

### Configuration으로 실행
```json
{
    "year": 2024,
    "month": 3
}
```

### 기본값
- Configuration이 없으면 현재 달 처리

## 파이프라인 단계

### 1단계: 법령 목록 수집
- 지정된 년/월의 법령 목록을 법제처 API에서 수집
- S3에 페이지별 JSON 파일로 저장
- **출력**: `s3://bucket/eflaw_lists/YYYY-MM/`

### 2단계: 법령 본문 수집  
- 1단계에서 수집한 목록을 기반으로 각 법령의 상세 본문 수집
- 병렬 처리로 효율성 향상
- **출력**: `s3://bucket/eflaw_contents/YYYY-MM/`

### 3단계: 데이터 정제
- 2단계에서 수집한 본문 데이터를 CSV로 정제
- 텍스트 정제, 날짜 파싱, 유예기간 계산 등
- **출력**: `s3://bucket/processed_data/YYYY-MM/YYYY-MM_processed_eflaw_data.csv`

### 4단계: RDS 직접 저장 
- 3단계에서 생성한 CSV를 RDS PostgreSQL에 직접 저장
- Django 모델 스키마와 호환되는 테이블 구조
- 소관부처, 법종구분 마스터 데이터 자동 생성
- 처리 로그 및 통계 정보 자동 생성
- **출력**: RDS PostgreSQL 테이블
"""