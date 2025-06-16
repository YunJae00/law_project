# 법령 데이터 수집 및 카테고리별 우선순위 분석 시스템
일반인을 위한 맞춤형 법령 정보 서비스

## 소개
본 프로젝트는 법제처 API를 활용하여 월별 법령 데이터를 수집하고, **13개 카테고리별 맞춤형 점수 알고리즘**을 통해 일반인에게 실질적 영향을 미치는 법령을 선별하는 데이터 파이프라인입니다.

복잡한 법령 정보를 **데이터 기반으로 우선순위화**하여 일반인도 쉽게 이해할 수 있는 정보로 변환함으로써 **법령 정보 접근성 민주화**를 실현합니다. 월평균 350건의 법령 데이터를 자동으로 수집·처리하여 각 카테고리별 상위 5개 법령을 제공합니다.

## 주요 기능

### 핵심 기능
- **4단계 ETL 파이프라인**
  - 법령 목록 수집 (`monthly-law-collector`)
  - 법령 본문 수집 (`monthly-law-contents-collector`)
  - 데이터 정제 (`monthly-law-processor`)
  - RDS 저장 & 카테고리별 점수화 (`monthly_law_data_to_rds`)

- **13개 카테고리별 맞춤형 점수 알고리즘**
  - 의료/건강, 복지/사회보장, 경제/세무, 근로/고용
  - 교육/학술, 가족/육아, 주택/부동산, 교통/운송
  - 환경/안전, 문화/여가, 사업/창업, 전문/특수, 기타

- **완전 자동화된 데이터 처리**
  - Apache Airflow 기반 워크플로우 관리
  - AWS Lambda 서버리스 처리
  - 병렬 처리를 통한 35% 성능 향상

- **하이브리드 데이터 저장 아키텍처**
  - S3 데이터 레이크 (원본 데이터 보존)
  - PostgreSQL RDS (구조화된 데이터)
  - Django ORM 호환 테이블 구조

## 시스템 아키텍처

![image](https://github.com/user-attachments/assets/07f07be6-1480-4c12-b0af-7a3b38163665)

**S3 데이터 저장 구조:**
```
airflow-law-raw-data/
├── eflaw_lists/           # 법령 목록 원본 데이터
│   └── YYYY-MM/
├── eflaw_contents/        # 법령 본문 원본 데이터
│   └── YYYY-MM/
└── processed_data/        # 정제된 CSV 데이터
    └── YYYY-MM/
```

## 기술 스택

### 데이터 엔지니어링
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![AWS Lambda](https://img.shields.io/badge/AWS%20Lambda-FF9900?style=for-the-badge&logo=aws-lambda&logoColor=white)
![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/postgres-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white)

### 클라우드 & 인프라
![AWS](https://img.shields.io/badge/AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Amazon S3](https://img.shields.io/badge/Amazon%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white)
![AWS ECR](https://img.shields.io/badge/AWS%20ECR-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)

### 백엔드 개발
![Django](https://img.shields.io/badge/django-%23092E20.svg?style=for-the-badge&logo=django&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)

### 데이터 분석 & 시각화
![Power BI](https://img.shields.io/badge/power_bi-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)
![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)

### 개발 예정
![Selenium](https://img.shields.io/badge/-selenium-%43B02A?style=for-the-badge&logo=selenium&logoColor=white)

## 주요 성과

- **월평균 350건** 법령 데이터 자동 수집 및 처리
- **95% 이상 데이터 품질** 달성 (정제 후 일관성 기준)
- **병렬 처리 도입**으로 처리 시간 **35% 단축**
- **AWS 프리티어** 환경에서 월 비용 **$0** 운영
- **13개 카테고리별** 상위 5개 법령 자동 선별 (총 65건/월)

## 데이터 시각화

### Tableau 대시보드 (진행 중)
- **키워드 빈도 분석**: 법령명과 제개정이유에서 가장 많이 사용된 단어 추출
- **카테고리별 키워드 분포**: 13개 카테고리별 주요 키워드와 빈도수
- **유예기간 분석**: 카테고리별 평균 유예기간 및 긴급성 지표
- **중요도 지수**: 키워드 포함 법령의 평균 점수를 통한 사회적 관심도 측정
- **시계열 트렌드**: 월별 카테고리 비중 변화 및 주요 이슈 식별

## 향후 계획

- **Selenium 뉴스 크롤링**: 법령 관련 뉴스 기사 수집으로 맥락적 정보 보강
- **Django 웹서비스 완성**: 사용자 맞춤형 법령 정보 제공
- **실시간 알림 시스템**: 중요 법령 변경사항 알림
- **머신러닝 기반 점수 개선**: 사용자 피드백 학습
