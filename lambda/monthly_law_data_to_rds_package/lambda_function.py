import json
import logging
import os
from datetime import datetime
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from io import StringIO

# Lambda용 로거
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class RDSLawDataLoader:
    """RDS 직접 저장용 법령 데이터 로더 (카테고리별 점수화 포함)"""

    def __init__(self):
        self.bucket_name = ''
        self.s3_client = boto3.client('s3')

        self.db_config = {
            # ⚠️ 나중에 git 체크 필요
            'host': os.environ.get('DB_HOST', ''),
            'port': int(os.environ.get('DB_PORT', '5432')),
            'database': os.environ.get('DB_NAME', ''),
            'user': os.environ.get('DB_USER', 'postgres'),
            'password': os.environ.get('DB_PASSWORD', '')
        }

        self.success_count = 0
        self.error_count = 0

    def get_db_connection(self):
        """데이터베이스 연결"""
        try:
            connection = psycopg2.connect(**self.db_config)
            return connection
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            raise

    def create_django_compatible_tables(self):
        """Django 호환 테이블 생성 (CASCADE 설정 추가해서 한거)"""
        create_tables_sql = """
        -- 법령 데이터 메인 테이블
        CREATE TABLE IF NOT EXISTS law_data (
            id SERIAL PRIMARY KEY,
            law_key VARCHAR(100),
            law_id VARCHAR(100),
            law_name_korean TEXT NOT NULL,
            law_name_chinese TEXT,
            law_name_abbreviation VARCHAR(500),
            promulgation_number VARCHAR(200),
            promulgation_date_str VARCHAR(20),
            promulgation_date DATE,
            enforcement_date_str VARCHAR(20),
            enforcement_date DATE,
            grace_period_days INTEGER,
            revision_type VARCHAR(50),
            language VARCHAR(10),
            phone_number VARCHAR(50),
            chapter_section TEXT,
            is_promulgated_law VARCHAR(10),            
            revision_content TEXT,
            revision_reason TEXT,
            total_articles INTEGER,
            total_addenda INTEGER,
            processing_month VARCHAR(10) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        );
    
        -- 카테고리 테이블
        CREATE TABLE IF NOT EXISTS law_category (
            id SERIAL PRIMARY KEY,
            code VARCHAR(20) UNIQUE NOT NULL,
            name VARCHAR(50) NOT NULL,
            description TEXT DEFAULT '',
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE
        );
        
        -- 처리된 법령 테이블
        CREATE TABLE IF NOT EXISTS processed_law (
            id SERIAL PRIMARY KEY,
            law_data_id INTEGER REFERENCES law_data(id) ON DELETE CASCADE,
            category_id INTEGER REFERENCES law_category(id) ON DELETE CASCADE,
            processing_month VARCHAR(7) NOT NULL,
            priority_score DOUBLE PRECISION NOT NULL,
            category_rank INTEGER NOT NULL,
            score_breakdown JSONB DEFAULT '{}',
            category_scores JSONB DEFAULT '{}',
            impact_keywords JSONB DEFAULT '[]',
            created_at TIMESTAMP WITH TIME ZONE,
            UNIQUE(processing_month, category_id, category_rank)
        );
    
        -- 인덱스 생성
        CREATE INDEX IF NOT EXISTS idx_law_data_law_key ON law_data(law_key);
        CREATE INDEX IF NOT EXISTS idx_law_data_processing_month ON law_data(processing_month);
        CREATE INDEX IF NOT EXISTS idx_processed_law_month_category ON processed_law(processing_month, category_id);
        CREATE INDEX IF NOT EXISTS idx_processed_law_rank ON processed_law(category_rank);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_law_data_unique_key_month ON law_data(law_key, processing_month);
        """

        # 카테고리 데이터 삽입
        insert_categories_sql = """
        INSERT INTO law_category (code, name, description, created_at, updated_at) VALUES
        ('HEALTH', '의료/건강', '의료, 건강, 병원, 약국 관련', NOW(), NOW()),
        ('WELFARE', '복지/사회보장', '복지, 연금, 보험, 지원 관련', NOW(), NOW()),
        ('TAX', '경제/세무', '세금, 소득, 관세 관련', NOW(), NOW()),
        ('LABOR', '근로/고용', '근로, 임금, 고용 관련', NOW(), NOW()),
        ('EDUCATION', '교육/학술', '교육, 학교, 연구 관련', NOW(), NOW()),
        ('FAMILY', '가족/육아', '가족, 육아, 출산 관련', NOW(), NOW()),
        ('HOUSING', '주택/부동산', '주택, 부동산, 임대 관련', NOW(), NOW()),
        ('TRANSPORT', '교통/운송', '교통, 운송, 도로 관련', NOW(), NOW()),
        ('ENVIRONMENT', '환경/안전', '환경, 안전, 재해 관련', NOW(), NOW()),
        ('CULTURE', '문화/여가', '문화, 관광, 체육 관련', NOW(), NOW()),
        ('BUSINESS', '사업/창업', '사업, 창업, 소상공인 관련', NOW(), NOW()),
        ('SPECIAL', '전문/특수', '군사, 외교, 전문기술 관련', NOW(), NOW()),
        ('ETC', '기타', '기타 분류되지 않은 법령', NOW(), NOW())
        ON CONFLICT (code) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description,
            updated_at = NOW();
        """

        conn = None
        cursor = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()

            # 테이블 생성
            cursor.execute(create_tables_sql)
            logger.info("테이블 생성 완료 (CASCADE 설정 포함)")

            # 카테고리 데이터 삽입
            cursor.execute(insert_categories_sql)
            logger.info("카테고리 데이터 삽입 완료")

            conn.commit()
            logger.info("Django 호환 테이블 (CASCADE 포함) 생성/확인 완료")

        except Exception as e:
            logger.error(f"테이블 생성 실패: {e}")
            if conn:
                conn.rollback()
                logger.info("트랜잭션 롤백 완료")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    # =====================================================
    # 카테고리 분류 로직 (모든 법률을 카테고리를 해당시켜서 나중에 확인)
    # =====================================================

    def determine_primary_category(self, row):
        """모든 카테고리별로 점수를 매겨서 최고점 카테고리 선택"""

        law_name = str(row.get('법령명_한글', '')).lower()
        dept = str(row.get('소관부처', ''))
        content = str(row.get('제개정이유내용', '')).lower()

        category_scores = {}

        # === 1. 의료/건강 점수 ==
        health_score = 0
        health_keywords = ['의료', '건강', '병원', '약', '치료', '진료', '환자', '질병', '수술', '응급']
        health_score += sum(3 for keyword in health_keywords if keyword in law_name)
        health_score += sum(1 for keyword in health_keywords if keyword in content)
        if dept == '보건복지부':
            health_score += 5
        category_scores['HEALTH'] = health_score

        # === 2. 복지/사회보장 점수 ===
        welfare_score = 0
        welfare_keywords = ['복지', '지원', '급여', '연금', '보험', '생계', '주거급여', '의료급여', '교육급여', '장애', '노인']
        welfare_score += sum(3 for keyword in welfare_keywords if keyword in law_name)
        welfare_score += sum(1 for keyword in welfare_keywords if keyword in content)
        if dept in ['보건복지부', '국가보훈부']:
            welfare_score += 5
        category_scores['WELFARE'] = welfare_score

        # === 3. 경제/세무 점수 ===
        tax_score = 0
        tax_keywords = ['세금', '소득', '부가가치', '관세', '과세', '세무', '신고', '공제', '감면']
        tax_score += sum(3 for keyword in tax_keywords if keyword in law_name)
        tax_score += sum(1 for keyword in tax_keywords if keyword in content)
        if dept == '기획재정부':
            tax_score += 5
        category_scores['TAX'] = tax_score

        # === 4. 근로/고용 점수 ===
        labor_score = 0
        labor_keywords = ['근로', '임금', '고용', '노동', '최저임금', '근로시간', '휴가', '해고', '산재']
        labor_score += sum(3 for keyword in labor_keywords if keyword in law_name)
        labor_score += sum(1 for keyword in labor_keywords if keyword in content)
        if dept == '고용노동부':
            labor_score += 5
        category_scores['LABOR'] = labor_score

        # === 5. 교육/학술 점수 ===
        education_score = 0
        education_keywords = ['교육', '학교', '학생', '대학', '연구', '학습', '교사', '수업']
        education_score += sum(3 for keyword in education_keywords if keyword in law_name)
        education_score += sum(1 for keyword in education_keywords if keyword in content)
        if dept == '교육부':
            education_score += 5
        category_scores['EDUCATION'] = education_score

        # === 6. 가족/육아 점수 ===
        family_score = 0
        family_keywords = ['가족', '육아', '출산', '아동', '청소년', '보육', '임신', '교육급여', '가구원']
        family_score += sum(3 for keyword in family_keywords if keyword in law_name)
        family_score += sum(1 for keyword in family_keywords if keyword in content)
        if dept == '여성가족부':
            family_score += 5
        category_scores['FAMILY'] = family_score

        # === 7. 주택/부동산 점수 ===
        housing_score = 0
        housing_keywords = ['주택', '부동산', '임대', '전세', '매매', '분양', '건설']
        housing_score += sum(3 for keyword in housing_keywords if keyword in law_name)
        housing_score += sum(1 for keyword in housing_keywords if keyword in content)
        if dept == '국토교통부':
            housing_score += 5
        category_scores['HOUSING'] = housing_score

        # === 8. 교통/운송 점수 ===
        transport_score = 0
        transport_keywords = ['교통', '운송', '도로', '자동차', '운전', '대중교통', '지하철', '버스']
        transport_score += sum(3 for keyword in transport_keywords if keyword in law_name)
        transport_score += sum(1 for keyword in transport_keywords if keyword in content)
        if dept == '국토교통부':
            transport_score += 3  # 주택과 겹치므로 낮은 점수
        category_scores['TRANSPORT'] = transport_score

        # === 9. 환경/안전 점수 ===
        environment_score = 0
        environment_keywords = ['환경', '안전', '재해', '오염', '화학', '폐기물', '기후']
        environment_score += sum(3 for keyword in environment_keywords if keyword in law_name)
        environment_score += sum(1 for keyword in environment_keywords if keyword in content)
        if dept == '환경부':
            environment_score += 5
        category_scores['ENVIRONMENT'] = environment_score

        # === 10. 문화/여가 점수 ===
        culture_score = 0
        culture_keywords = ['문화', '관광', '체육', '예술', '스포츠', '문화재', '박물관']
        culture_score += sum(3 for keyword in culture_keywords if keyword in law_name)
        culture_score += sum(1 for keyword in culture_keywords if keyword in content)
        if dept == '문화체육관광부':
            culture_score += 5
        category_scores['CULTURE'] = culture_score

        # === 11. 사업/창업 점수 ===
        business_score = 0
        business_keywords = ['사업', '창업', '소상공인', '중소기업', '기업', '상업', '영업']
        business_score += sum(3 for keyword in business_keywords if keyword in law_name)
        business_score += sum(1 for keyword in business_keywords if keyword in content)
        if dept == '중소벤처기업부':
            business_score += 5
        category_scores['BUSINESS'] = business_score

        # === 12. 전문/특수 점수 ===
        special_score = 0
        special_keywords = ['고엽제', '보훈', '군사', '국방', '외교', '통계', '측량', '특허']
        special_score += sum(3 for keyword in special_keywords if keyword in law_name)
        special_score += sum(1 for keyword in special_keywords if keyword in content)
        if dept in ['국가보훈부', '국방부', '외교부']:
            special_score += 5
        category_scores['SPECIAL'] = special_score

        # === 최고점 카테고리 선택 (아니면 ETC) ===+
        if not any(score > 0 for score in category_scores.values()):
            return 'ETC', category_scores

        primary_category = max(category_scores, key=category_scores.get)

        return primary_category, category_scores

    # =====================================================
    # 카테고리별 점수화 로직
    # =====================================================

    def calculate_health_priority(self, law):
        """의료/건강: 생명 > 접근성 > 예방 순서로 중요도 설정 (이거 나중에 단어/점수 체계화 할 예정)"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']
        score = 0

        # === Tier 1: 생명 직결 (60점) ===
        # 응급상황, 생명위험, 중대질병
        life_critical = {
            '응급의료': 60, '생명위험': 60, '중환자': 55,
            '암': 55, '심장': 55, '뇌': 55, '수술': 50,
            '중독': 50, '감염병': 55, '전염병': 55,
            '의료사고': 50, '환자안전': 50
        }

        # === Tier 2: 치료 접근성 (40점) ===
        # 의료비, 의료서비스 접근
        access_related = {
            '의료비': 40, '진료비': 40, '수술비': 40,
            '건강보험': 38, '의료급여': 38, '본인부담': 35,
            '의료서비스': 35, '진료': 35, '치료': 35,
            '병원': 30, '의원': 30, '약국': 30
        }

        # === Tier 3: 예방/관리 (25점) ===
        # 건강관리, 예방접종
        prevention_related = {
            '예방접종': 25, '건강검진': 25, '예방': 20,
            '건강관리': 20, '보건': 18, '위생': 18,
            '건강증진': 15, '상담': 15
        }

        # 점수 계산 (최고점만 적용)
        max_score = 0
        found_keyword = ""

        all_keywords = {**life_critical, **access_related, **prevention_related}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # === 대상자 가중치 (×1.0~1.5) 각각 중요한걸 따짐 ===
        multiplier = 1.0
        if any(word in content for word in ['전국민', '모든환자', '전체']):
            multiplier = 1.5
        elif any(word in content for word in ['아동', '노인', '임산부', '장애인']):
            multiplier = 1.3
        elif any(word in content for word in ['환자', '의료진']):
            multiplier = 1.2

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),  # 최대 100점
            'primary_keyword': found_keyword,
            'tier': 'life_critical' if max_score >= 50 else 'access' if max_score >= 30 else 'prevention',
            'multiplier': multiplier
        }

    def calculate_tax_priority(self, law):
        """경제/세무: 실질소득 영향 > 납세편의 > 세제 공정성"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 실질소득 직접 영향 (70점) ===
        # 개인/기업의 실제 돈에 바로 영향
        income_impact = {
            '소득세': 70, '종합소득세': 70, '근로소득세': 70,
            '부가가치세': 65, '법인세': 60, '종합부동산세': 60,
            '세율': 65, '공제': 60, '감면': 60, '면제': 55,
            '최저임금': 70, '연금': 65, '보험료': 60
        }

        # === Tier 2: 납세 편의성 (45점) ===
        # 세무처리 절차, 신고 관련
        convenience = {
            '세무신고': 45, '신고절차': 45, '전자신고': 40,
            '간소화': 40, '원스톱': 40, '디지털': 35,
            '신고기한': 35, '납부방법': 30, '서류': 30
        }

        # === Tier 3: 세제 공정성 (30점) ===
        # 조세정의, 형평성
        fairness = {
            '세제개편': 30, '공정과세': 30, '형평성': 25,
            '조세회피': 25, '탈세': 25, '투명성': 20,
            '과세형평': 20
        }

        # 점수 계산
        max_score = 0
        found_keyword = ""

        all_keywords = {**income_impact, **convenience, **fairness}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # === 대상자 규모 가중치 계산 ===
        multiplier = 1.0
        if any(word in content for word in ['전체납세자', '모든국민', '전국민']):
            multiplier = 1.4
        elif any(word in content for word in ['근로자', '사업자', '개인사업자']):
            multiplier = 1.3
        elif any(word in content for word in ['중소기업', '소상공인']):
            multiplier = 1.2
        elif any(word in content for word in ['대기업', '고소득자']):
            multiplier = 1.1

        # === 변화 방향 보정 ===
        direction_bonus = 0
        if any(word in content for word in ['인하', '감면', '지원', '혜택']):
            direction_bonus = 5  # 국민 부담 감소
        elif any(word in content for word in ['인상', '신설']):
            direction_bonus = 3  # 부담 증가도 중요함

        final_score = (max_score * multiplier) + direction_bonus

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'income_impact' if max_score >= 55 else 'convenience' if max_score >= 35 else 'fairness',
            'multiplier': multiplier,
            'direction_bonus': direction_bonus
        }

    def calculate_labor_priority(self, law):
        """근로/고용: 기본권 > 근로조건 > 고용안정 > 근로환경"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 노동 기본권 (80점) ===
        # 헌법적 권리, 생존권과 직결
        basic_rights = {
            '최저임금': 80, '부당해고': 80, '노동권': 75,
            '단결권': 75, '단체교섭': 75, '단체행동': 75,
            '임금': 70, '급여': 70, '수당': 65,
            '근로시간': 70, '휴게시간': 65, '연장근로': 65
        }

        # === Tier 2: 근로조건 (55점) ===
        # 근로환경, 복리후생
        work_conditions = {
            '휴가': 55, '연차': 55, '병가': 55, '출산휴가': 55,
            '육아휴직': 55, '근로환경': 50, '안전': 50,
            '산업재해': 55, '산재': 55, '직업병': 50,
            '복리후생': 45, '퇴직금': 50, '퇴직': 45
        }

        # === Tier 3: 고용안정 (40점) ===
        # 고용기회, 고용보험
        employment_stability = {
            '고용': 40, '채용': 40, '취업': 40,
            '고용보험': 45, '실업급여': 45, '재취업': 40,
            '직업훈련': 35, '취업지원': 35, '일자리': 40
        }

        # === Tier 4: 특별 보호 (50점) ===
        # 취약근로자 보호 (높은 점수)
        special_protection = {
            '비정규직': 50, '파견근로': 50, '특수고용': 50,
            '플랫폼노동': 50, '프리랜서': 45, '일용직': 45,
            '외국인근로자': 45, '장애인고용': 50, '여성근로': 45
        }

        # 점수 계산
        max_score = 0
        found_keyword = ""

        all_keywords = {**basic_rights, **work_conditions, **employment_stability, **special_protection}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # === 근로자 규모 가중치 ===
        multiplier = 1.0
        if any(word in content for word in ['전체근로자', '모든직장인', '전국']):
            multiplier = 1.4
        elif any(word in content for word in ['정규직', '상용직']):
            multiplier = 1.3
        elif any(word in content for word in ['비정규직', '임시직', '일용직']):
            multiplier = 1.2  # 취약계층 보호 중요

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'basic_rights' if max_score >= 65 else 'conditions' if max_score >= 45 else 'stability',
            'multiplier': multiplier
        }

    def calculate_welfare_priority(self, law):
        """복지/사회보장: 생존보장 > 기본생활 > 사회참여"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 생존 보장 (85점) ===
        # 기초생활, 생존권 직결
        survival_guarantee = {
            '기초생활수급': 85, '생계급여': 85, '의료급여': 80,
            '주거급여': 80, '교육급여': 75, '긴급지원': 80,
            '기초연금': 75, '국민연금': 75, '장애연금': 75,
            '아동수당': 70, '양육수당': 70, '보육료': 70
        }

        # === Tier 2: 기본 생활 지원 (60점) ===
        # 일상생활 지원, 서비스 접근
        basic_living = {
            '돌봄서비스': 60, '재가서비스': 60, '방문서비스': 55,
            '장애인활동지원': 60, '노인장기요양': 60, '치매': 55,
            '보건소': 50, '복지관': 50, '상담': 45,
            '사회복지시설': 50, '복지서비스': 50
        }

        # === Tier 3: 사회참여 지원 (40점) ===
        # 자립, 사회통합
        social_participation = {
            '자립지원': 40, '취업지원': 40, '직업재활': 40,
            '사회복귀': 35, '재활': 35, '자활': 40,
            '사회통합': 35, '문화활동': 30, '여가': 25
        }

        # 점수 계산
        max_score = 0
        found_keyword = ""

        all_keywords = {**survival_guarantee, **basic_living, **social_participation}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # === 대상자 취약성 가중치 ===
        multiplier = 1.0
        if any(word in content for word in ['기초생활수급자', '차상위', '저소득']):
            multiplier = 1.5  # 가장 취약한 계층
        elif any(word in content for word in ['장애인', '노인', '아동', '한부모']):
            multiplier = 1.3  # 사회적 약자
        elif any(word in content for word in ['임산부', '영유아', '청소년']):
            multiplier = 1.2

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'survival' if max_score >= 70 else 'basic_living' if max_score >= 45 else 'participation',
            'multiplier': multiplier
        }

    def calculate_special_priority(self, law):
        """전문/특수: 국가안보 > 공공안전 > 전문기술 > 행정효율 (todo: 이건 좀 애매한 구석이 있음)"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 국가안보 (90점) ===
        # 국가 존립, 주권과 직결
        national_security = {
            '국가기밀': 90, '안보': 90, '국방': 85, '군사': 85,
            '첩보': 85, '보안': 80, '외교': 80, '국익': 80,
            '테러': 85, '간첩': 85, '국가정보': 80
        }

        # === Tier 2: 공공안전 (70점) ===
        # 사회 안전, 재해 대응
        public_safety = {
            '재해대응': 70, '재난': 70, '응급상황': 70,
            '화재': 65, '지진': 65, '홍수': 65, '태풍': 65,
            '식품안전': 60, '환경오염': 60, '방사능': 70,
            '전염병': 65, '공중보건': 60
        }

        # === Tier 3: 전문기술 (50점) ===
        # 국가 경쟁력, 기술 발전
        professional_tech = {
            '연구개발': 50, '기술개발': 50, '특허': 45,
            '지식재산': 45, '기술이전': 40, '혁신': 45,
            '인공지능': 45, '빅데이터': 40, '블록체인': 40,
            '바이오': 45, '우주': 50, '원자력': 55
        }

        # === Tier 4: 행정효율 (35점) ===
        # 정부 운영, 행정 개선
        administrative = {
            '행정절차': 35, '전자정부': 35, '디지털정부': 40,
            '규제개혁': 40, '행정효율': 35, '민원': 30,
            '정부조직': 30, '공무원': 30, '인사': 25
        }

        # 점수 계산
        max_score = 0
        found_keyword = ""

        all_keywords = {**national_security, **public_safety, **professional_tech, **administrative}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # === 파급효과 가중치 ===
        multiplier = 1.0
        if any(word in content for word in ['전국', '전면', '전체', '국가적']):
            multiplier = 1.3
        elif any(word in content for word in ['지역', '부문', '분야']):
            multiplier = 1.1

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'national_security' if max_score >= 80 else 'public_safety' if max_score >= 60 else 'professional_tech' if max_score >= 40 else 'administrative',
            'multiplier': multiplier
        }

    def calculate_education_priority(self, law):
        """교육/학술: 교육기회 > 교육비용 > 교육품질 > 학술발전"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 교육 기회 평등 (80점) ===
        educational_opportunity = {
            '의무교육': 80, '무상교육': 80, '교육기회': 75,
            '입학': 70, '진학': 70, '교육접근': 75,
            '특수교육': 75, '다문화교육': 70, '평생교육': 65,
            '교육격차': 75, '교육불평등': 75
        }

        # === Tier 2: 교육 비용 (65점) ===
        educational_cost = {
            '등록금': 65, '수업료': 65, '교육비': 60,
            '장학금': 60, '학자금': 60, '교육지원': 55,
            '급식': 55, '교재': 50, '교복': 45
        }

        # === Tier 3: 교육 품질 (50점) ===
        educational_quality = {
            '교육과정': 50, '교육내용': 50, '교육방법': 45,
            '교사': 50, '교원': 50, '교육시설': 45,
            '교육환경': 45, '학급규모': 45, '교육자료': 40
        }

        # === Tier 4: 학술 발전 (40점) ===
        academic_development = {
            '연구': 40, '학술': 40, '논문': 35,
            '학회': 35, '연구비': 40, '학술지': 30,
            '연구윤리': 35, '학문의자유': 45
        }

        max_score = 0
        found_keyword = ""

        all_keywords = {**educational_opportunity, **educational_cost, **educational_quality, **academic_development}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # 대상자 가중치
        multiplier = 1.0
        if any(word in content for word in ['모든학생', '전체학생', '전국']):
            multiplier = 1.4
        elif any(word in content for word in ['초등학생', '중학생', '고등학생']):
            multiplier = 1.3
        elif any(word in content for word in ['대학생', '대학원생']):
            multiplier = 1.2
        elif any(word in content for word in ['유아', '아동']):
            multiplier = 1.2

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'opportunity' if max_score >= 70 else 'cost' if max_score >= 55 else 'quality' if max_score >= 45 else 'academic',
            'multiplier': multiplier
        }

    def calculate_family_priority(self, law):
        """가족/육아: 육아지원 > 가족보호 > 가족형성 > 가족문화"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 육아 지원 (85점) ===
        childcare_support = {
            '보육료': 85, '어린이집': 80, '유치원': 80,
            '육아휴직': 80, '출산휴가': 80, '출산': 75,
            '임신': 75, '신생아': 75, '영유아': 75,
            '보육': 70, '양육': 70, '육아': 70,
            '아이돌봄': 75, '육아용품': 65
        }

        # === Tier 2: 가족 보호 (70점) ===
        family_protection = {
            '가정폭력': 70, '아동학대': 75, '아동보호': 75,
            '가족보호': 65, '한부모': 70, '조손가정': 65,
            '다문화가정': 65, '입양': 60, '가정위탁': 60,
            '이혼': 55, '양육비': 65, '면접교섭': 50
        }

        # === Tier 3: 가족 형성 지원 (55점) ===
        family_formation = {
            '결혼': 55, '신혼부부': 60, '청년': 50,
            '혼인': 50, '가족구성': 45, '동거': 40,
            '사실혼': 45, '가족관계': 40
        }

        # === Tier 4: 가족 문화 (40점) ===
        family_culture = {
            '가족문화': 40, '가족여가': 35, '가족교육': 40,
            '가족상담': 35, '가족치료': 40, '부모교육': 35
        }

        max_score = 0
        found_keyword = ""

        all_keywords = {**childcare_support, **family_protection, **family_formation, **family_culture}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # 대상자 가중치
        multiplier = 1.0
        if any(word in content for word in ['모든가정', '전체가족', '전국']):
            multiplier = 1.4
        elif any(word in content for word in ['영유아가정', '신생아가정']):
            multiplier = 1.3
        elif any(word in content for word in ['한부모', '조손', '다문화']):
            multiplier = 1.3  # 취약가정 우선
        elif any(word in content for word in ['맞벌이', '육아맘']):
            multiplier = 1.2

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'childcare' if max_score >= 70 else 'protection' if max_score >= 60 else 'formation' if max_score >= 45 else 'culture',
            'multiplier': multiplier
        }

    def calculate_housing_priority(self, law):
        """주택/부동산: 주거안정 > 주택공급 > 부동산시장 > 주거환경"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 주거 안정 (85점) ===
        housing_stability = {
            '임대차': 85, '전세': 80, '월세': 80,
            '임대료': 80, '보증금': 75, '계약갱신': 75,
            '주거안정': 85, '강제퇴거': 80, '임차인보호': 75,
            '공공임대': 70, '사회주택': 70, '임대주택': 70
        }

        # === Tier 2: 주택 공급 (65점) ===
        housing_supply = {
            '주택공급': 65, '신규주택': 60, '주택건설': 60,
            '택지개발': 55, '도시개발': 55, '재개발': 60,
            '재건축': 60, '주택용지': 50, '분양': 55
        }

        # === Tier 3: 부동산 시장 (55점) ===
        real_estate_market = {
            '부동산가격': 55, '주택가격': 60, '집값': 60,
            '매매': 50, '거래': 50, '중개': 45,
            '부동산중개': 45, '공시지가': 45, '감정평가': 40
        }

        # === Tier 4: 주거 환경 (45점) ===
        living_environment = {
            '주거환경': 45, '주택성능': 40, '에너지효율': 40,
            '리모델링': 35, '주택개량': 35, '주거복지': 50,
            '주택품질': 40, '건축': 35
        }

        max_score = 0
        found_keyword = ""

        all_keywords = {**housing_stability, **housing_supply, **real_estate_market, **living_environment}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # 대상자 가중치
        multiplier = 1.0
        if any(word in content for word in ['전국민', '모든시민']):
            multiplier = 1.4
        elif any(word in content for word in ['임차인', '세입자', '임대인']):
            multiplier = 1.3
        elif any(word in content for word in ['청년', '신혼부부', '1인가구']):
            multiplier = 1.3
        elif any(word in content for word in ['저소득', '취약계층']):
            multiplier = 1.4

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'stability' if max_score >= 70 else 'supply' if max_score >= 55 else 'market' if max_score >= 45 else 'environment',
            'multiplier': multiplier
        }

    def calculate_transport_priority(self, law):
        """교통/운송: 교통안전 > 교통비용 > 교통편의 > 교통환경"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 교통 안전 (90점) ===
        # 생명과 직결되는 교통안전이 최우선
        traffic_safety = {
            '교통사고': 90, '안전운전': 85, '교통안전': 85,
            '음주운전': 80, '무면허': 75, '과속': 75,
            '신호위반': 70, '보행자안전': 80, '어린이보호구역': 85,
            '스쿨존': 85, '횡단보도': 75, '교통신호': 70,
            '자동차안전': 70, '차량검사': 65, '안전벨트': 65
        }

        # === Tier 2: 교통 비용 (70점) ===
        # 서민 교통비 부담과 직결
        traffic_cost = {
            '대중교통요금': 70, '지하철요금': 70, '버스요금': 70,
            '택시요금': 65, '통행료': 65, '주차요금': 60,
            '교통카드': 60, '교통비': 65, '유류비': 60,
            '자동차세': 55, '교통세': 55
        }

        # === Tier 3: 교통 편의 (55점) ===
        # 교통 접근성, 편리성
        traffic_convenience = {
            '대중교통': 55, '지하철': 50, '버스': 50,
            '교통편의': 50, '환승': 45, '교통카드': 45,
            '교통정보': 40, '길안내': 35, '교통앱': 35,
            '무장애': 55, '교통약자': 60, '휠체어': 55,
            '저상버스': 50, '엘리베이터': 45
        }

        # === Tier 4: 교통 환경 (45점) ===
        # 친환경 교통, 교통체계
        traffic_environment = {
            '친환경교통': 45, '전기차': 40, '수소차': 40,
            '자전거': 40, '도보': 35, '보행': 40,
            '교통체계': 35, '교통계획': 30, '도로건설': 35,
            '교통인프라': 40, '스마트교통': 35
        }

        max_score = 0
        found_keyword = ""

        all_keywords = {**traffic_safety, **traffic_cost, **traffic_convenience, **traffic_environment}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # 대상자 가중치
        multiplier = 1.0
        if any(word in content for word in ['전국민', '모든시민', '전체']):
            multiplier = 1.4
        elif any(word in content for word in ['운전자', '보행자', '승객']):
            multiplier = 1.3
        elif any(word in content for word in ['교통약자', '장애인', '고령자']):
            multiplier = 1.3
        elif any(word in content for word in ['어린이', '학생', '청소년']):
            multiplier = 1.3

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'safety' if max_score >= 70 else 'cost' if max_score >= 60 else 'convenience' if max_score >= 45 else 'environment',
            'multiplier': multiplier
        }

    def calculate_environment_priority(self, law):
        """환경/안전: 생명위험 > 환경보호 > 재해대응 > 환경관리"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 생명 위험 환경 (95점) ===
        # 즉각적인 생명 위험과 직결
        life_threatening = {
            '화학물질': 95, '독성물질': 95, '방사능': 95,
            '대기오염': 85, '미세먼지': 85, '초미세먼지': 85,
            '수질오염': 80, '식수': 85, '상수도': 80,
            '토양오염': 75, '소음': 70, '진동': 65,
            '석면': 90, '중금속': 85, '발암물질': 90
        }

        # === Tier 2: 환경 보호 (70점) ===
        # 장기적 환경 건강성
        environmental_protection = {
            '기후변화': 70, '온실가스': 70, '탄소배출': 65,
            '재생에너지': 60, '에너지효율': 55, '친환경': 60,
            '생물다양성': 55, '생태계': 60, '자연보호': 55,
            '산림': 50, '해양': 55, '습지': 50,
            '환경보전': 65, '환경복원': 60
        }

        # === Tier 3: 재해 대응 (80점) ===
        # 자연재해, 환경재해 대응
        disaster_response = {
            '자연재해': 80, '태풍': 75, '홍수': 80,
            '지진': 85, '산사태': 75, '가뭄': 70,
            '화재': 80, '폭발': 85, '누출': 80,
            '환경사고': 75, '재해대응': 75, '응급대응': 80,
            '대피': 75, '구조': 80
        }

        # === Tier 4: 환경 관리 (50점) ===
        # 환경 제도, 관리 체계
        environmental_management = {
            '환경영향평가': 50, '환경기준': 45, '배출기준': 50,
            '환경감시': 45, '환경모니터링': 45, '환경조사': 40,
            '폐기물': 55, '재활용': 50, '순환경제': 45,
            '환경교육': 40, '환경정보': 35
        }

        max_score = 0
        found_keyword = ""

        all_keywords = {**life_threatening, **environmental_protection, **disaster_response, **environmental_management}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # 대상자 가중치
        multiplier = 1.0
        if any(word in content for word in ['전국민', '모든시민', '전국']):
            multiplier = 1.4
        elif any(word in content for word in ['지역주민', '인근주민']):
            multiplier = 1.3
        elif any(word in content for word in ['어린이', '임산부', '노인']):
            multiplier = 1.3  # 환경에 취약한 계층
        elif any(word in content for word in ['근로자', '작업자']):
            multiplier = 1.2

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'life_threatening' if max_score >= 80 else 'protection' if max_score >= 60 else 'disaster' if max_score >= 70 else 'management',
            'multiplier': multiplier
        }

    def calculate_culture_priority(self, law):
        """문화/여가: 문화접근성 > 문화다양성 > 문화산업 > 문화유산"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 문화 접근성 (75점) ===
        # 문화향유권, 문화 민주화
        cultural_accessibility = {
            '문화향유': 75, '문화접근': 70, '문화복지': 70,
            '문화바우처': 65, '문화이용료': 65, '관람료': 60,
            '무료관람': 70, '할인': 60, '문화시설': 60,
            '도서관': 65, '박물관': 60, '미술관': 60,
            '문화센터': 55, '공연장': 55, '영화관': 50
        }

        # === Tier 2: 문화 다양성 (65점) ===
        # 소수문화, 지역문화 보호
        cultural_diversity = {
            '다문화': 65, '문화다양성': 65, '소수문화': 60,
            '지역문화': 60, '전통문화': 60, '민속': 55,
            '문화정체성': 55, '문화교류': 50, '국제문화': 50,
            '청소년문화': 55, '노인문화': 50, '장애인문화': 60
        }

        # === Tier 3: 문화 산업 (50점) ===
        # 한류, 콘텐츠 산업
        cultural_industry = {
            '한류': 50, '콘텐츠': 50, '문화산업': 50,
            '게임': 45, '영화': 45, '음악': 45,
            '방송': 40, '출판': 40, '만화': 35,
            '문화기술': 45, '디지털문화': 45, 'K-pop': 50
        }

        # === Tier 4: 문화 유산 (60점) ===
        # 문화재, 유산 보호
        cultural_heritage = {
            '문화재': 60, '문화유산': 60, '유적': 55,
            '문화재보호': 55, '전통건축': 50, '민속자료': 45,
            '무형문화재': 55, '전통예술': 50, '전통기술': 45,
            '문화재복원': 50, '문화재관리': 45
        }

        max_score = 0
        found_keyword = ""

        all_keywords = {**cultural_accessibility, **cultural_diversity, **cultural_industry, **cultural_heritage}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # 대상자 가중치
        multiplier = 1.0
        if any(word in content for word in ['전국민', '모든시민', '전체']):
            multiplier = 1.3
        elif any(word in content for word in ['저소득', '취약계층', '소외계층']):
            multiplier = 1.4  # 문화 소외계층 우선
        elif any(word in content for word in ['청소년', '아동', '학생']):
            multiplier = 1.2
        elif any(word in content for word in ['노인', '장애인', '다문화']):
            multiplier = 1.3

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'accessibility' if max_score >= 60 else 'diversity' if max_score >= 55 else 'industry' if max_score >= 45 else 'heritage',
            'multiplier': multiplier
        }

    def calculate_business_priority(self, law):
        """사업/창업: 중소기업지원 > 창업지원 > 사업환경 > 기업규제"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']

        # === Tier 1: 중소기업 지원 (85점) ===
        # 서민경제 직결, 고용창출 핵심
        sme_support = {
            '소상공인': 85, '중소기업': 80, '자영업': 80,
            '소기업': 75, '마이크로': 75, '1인기업': 70,
            '중소기업지원': 80, '소상공인지원': 85, '자영업지원': 80,
            '경영지원': 65, '기술지원': 60, '자금지원': 75,
            '대출': 70, '신용보증': 70, '정책자금': 65
        }

        # === Tier 2: 창업 지원 (70점) ===
        # 일자리 창출, 경제 활성화
        startup_support = {
            '창업': 70, '스타트업': 70, '창업지원': 70,
            '창업자금': 65, '창업교육': 55, '창업보육': 60,
            '벤처기업': 65, '기술창업': 60, '청년창업': 70,
            '창업인큐베이터': 55, '액셀러레이터': 50, '펀딩': 60
        }

        # === Tier 3: 사업 환경 (60점) ===
        # 사업 편의성, 규제 완화
        business_environment = {
            '사업환경': 60, '영업환경': 55, '경영환경': 55,
            '규제완화': 60, '규제개선': 55, '행정절차': 50,
            '인허가': 55, '사업허가': 55, '영업허가': 50,
            '온라인신청': 45, '원스톱': 50, '디지털': 45,
            '전자상거래': 50, '온라인사업': 50
        }

        # === Tier 4: 기업 규제/제재 (40점) ===
        # 불공정 거래, 독점 방지
        business_regulation = {
            '공정거래': 40, '불공정거래': 45, '독점': 45,
            '담합': 40, '시장지배': 35, '경쟁제한': 35,
            '소비자보호': 50, '소비자피해': 50, '환불': 45,
            '품질보증': 40, '하자보수': 40, 'AS': 35
        }

        max_score = 0
        found_keyword = ""

        all_keywords = {**sme_support, **startup_support, **business_environment, **business_regulation}
        for keyword, points in all_keywords.items():
            if keyword in content:
                if points > max_score:
                    max_score = points
                    found_keyword = keyword

        # 대상자 가중치
        multiplier = 1.0
        if any(word in content for word in ['모든사업자', '전체기업', '전국']):
            multiplier = 1.3
        elif any(word in content for word in ['소상공인', '자영업자', '1인기업']):
            multiplier = 1.4  # 서민경제 직결
        elif any(word in content for word in ['중소기업', '소기업']):
            multiplier = 1.3
        elif any(word in content for word in ['창업자', '청년창업']):
            multiplier = 1.3
        elif any(word in content for word in ['대기업', '대규모기업']):
            multiplier = 1.0  # 일반 가중치

        final_score = max_score * multiplier

        return {
            'score': min(final_score, 100),
            'primary_keyword': found_keyword,
            'tier': 'sme_support' if max_score >= 70 else 'startup' if max_score >= 60 else 'environment' if max_score >= 50 else 'regulation',
            'multiplier': multiplier
        }

    def calculate_etc_priority(self, law):
        """기타: 일반적 기준으로 중요도 평가"""
        content = law['법령명_한글'] + ' ' + law['제개정이유내용']
        dept = law['소관부처']

        # === 부처별 기본 중요도 (서비스 대상에 따라 달라져야함 일단은 이 기준으로 가는거로)===
        dept_base_scores = {
            '보건복지부': 60, '기획재정부': 55, '고용노동부': 55,
            '교육부': 50, '여성가족부': 50, '국토교통부': 45,
            '환경부': 45, '문화체육관광부': 40, '농림축산식품부': 40,
            '산업통상자원부': 40, '과학기술정보통신부': 35,
            '행정안전부': 35, '법무부': 30, '외교부': 25,
            '국방부': 25, '통일부': 20
        }

        base_score = dept_base_scores.get(dept, 30)

        # === 법령 위계 가중치 ===
        law_type = law.get('법종구분', '')
        if law_type == '법률':
            base_score *= 1.3
        elif law_type == '대통령령':
            base_score *= 1.2
        elif law_type == '총리령':
            base_score *= 1.1

        # === 제개정 규모 가중치 ===
        revision_type = law.get('제개정구분', '')
        if revision_type == '제정':
            base_score *= 1.2
        elif revision_type == '전부개정':
            base_score *= 1.15
        elif revision_type == '일부개정':
            base_score *= 1.1

        # === 일반적 중요 키워드 보정 ===
        important_general = {
            '국민': 10, '시민': 8, '전국': 8, '전체': 6,
            '확대': 5, '신설': 6, '강화': 4, '개선': 4,
            '지원': 5, '혜택': 4, '서비스': 3, '편의': 3
        }

        keyword_bonus = 0
        for keyword, bonus in important_general.items():
            if keyword in content:
                keyword_bonus += bonus

        final_score = base_score + keyword_bonus

        return {
            'score': min(final_score, 100),
            'primary_keyword': f"{dept} 소관",
            'tier': 'general',
            'base_score': base_score,
            'keyword_bonus': keyword_bonus
        }

    def calculate_category_specific_priority(self, law, category):
        """카테고리별 맞춤 점수 계산 - 전체 카테고리 완성"""
        category_functions = {
            'HEALTH': self.calculate_health_priority,
            'WELFARE': self.calculate_welfare_priority,
            'TAX': self.calculate_tax_priority,
            'LABOR': self.calculate_labor_priority,
            'EDUCATION': self.calculate_education_priority,
            'FAMILY': self.calculate_family_priority,
            'HOUSING': self.calculate_housing_priority,
            'TRANSPORT': self.calculate_transport_priority,
            'ENVIRONMENT': self.calculate_environment_priority,
            'CULTURE': self.calculate_culture_priority,
            'BUSINESS': self.calculate_business_priority,
            'SPECIAL': self.calculate_special_priority,
            'ETC': self.calculate_etc_priority
        }

        # 해당 카테고리의 특화 함수가 있으면 사용, 없으면 일반 함수
        if category in category_functions:
            return category_functions[category](law)
        else:
            # 예외 처리: 알 수 없는 카테고리는 일반 점수로
            logger.warning(f"알 수 없는 카테고리: {category}, 일반 점수로 계산")
            return self.calculate_etc_priority(law)

    def extract_impact_keywords(self, law):
        """영향 키워드 추출"""
        content = str(law.get('제개정이유내용', ''))

        impact_keywords = []
        keyword_list = ['확대', '신설', '지원', '강화', '개선', '완화', '폐지', '인상', '인하']

        for keyword in keyword_list:
            if keyword in content:
                impact_keywords.append(keyword)

        return impact_keywords[:5]

    def load_csv_from_s3(self, s3_key):
        """S3에서 CSV 파일 로드"""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            logger.info(f"CSV 로드 완료: {len(df)} 레코드")
            return df
        except Exception as e:
            logger.error(f"CSV 로드 실패 ({s3_key}): {e}")
            return None

    def parse_date_safe(self, date_str):
        """날짜 파싱"""
        if pd.isna(date_str) or not date_str:
            return None

        try:
            if isinstance(date_str, str):
                date_str = date_str.strip()
                if len(date_str) == 8:  # YYYYMMDD
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                return date_str
            return None
        except Exception as e:
            logger.warning(f"날짜 파싱 실패 ({date_str}): {e}")
            return None

    # =====================================================
    # 통합 처리 메서드 (법령 데이터 + 카테고리별 점수화)
    # =====================================================

    def process_laws_with_categories(self, df, processing_month):
        """법령 데이터와 카테고리별 점수화를 함께 처리"""

        logger.info(f"카테고리별 법령 처리 시작: {len(df)}건")

        # 1단계: 법령 데이터 저장
        law_records = self.prepare_law_data_records(df, processing_month)
        law_data_ids = self.bulk_insert_law_data_with_ids(law_records)

        # 2단계: 카테고리별 분류 및 점수화
        category_results = {}

        for idx, row in df.iterrows():
            try:
                # 주 카테고리 결정
                primary_category, category_scores = self.determine_primary_category(row)

                # 카테고리별 점수 계산
                law_dict = row.to_dict()
                priority_result = self.calculate_category_specific_priority(law_dict, primary_category)

                # 영향 키워드 추출
                impact_keywords = self.extract_impact_keywords(law_dict)

                # 카테고리별 결과 저장
                if primary_category not in category_results:
                    category_results[primary_category] = []

                category_results[primary_category].append({
                    'law_key': row.get('법령키'),
                    'priority_score': priority_result['score'],
                    'score_breakdown': priority_result,
                    'category_scores': category_scores,
                    'impact_keywords': impact_keywords
                })

            except Exception as e:
                logger.error(f"법령 처리 실패 (행 {idx}): {e}")
                self.error_count += 1

        # 3단계: 각 카테고리별 상위 5개씩 DB 저장 (이건 일단 상위 5개만 보여주기 위함. todo: 다 저장하는거 고려해야함)
        processed_count = self.save_top_laws_by_category(category_results, processing_month)

        logger.info(f"카테고리별 법령 처리 완료: {processed_count}건 저장")

        return {
            'total_processed': len(df),
            'categories_found': len(category_results),
            'top_laws_saved': processed_count,
            'category_distribution': {cat: len(laws) for cat, laws in category_results.items()}
        }

    def prepare_law_data_records(self, df, processing_month):
        """법령 데이터 레코드 준비"""
        conn = self.get_db_connection()
        records = []

        try:
            for idx, row in df.iterrows():
                try:
                    # 날짜 파싱
                    promulgation_date = self.parse_date_safe(row.get('공포일자_datetime'))
                    enforcement_date = self.parse_date_safe(row.get('시행일자_datetime'))

                    # 레코드 생성
                    record = (
                        str(row.get('법령키', '')) if not pd.isna(row.get('법령키')) else None,
                        str(row.get('법령ID', '')) if not pd.isna(row.get('법령ID')) else None,
                        str(row.get('법령명_한글', '')),
                        str(row.get('법령명_한자', '')) if not pd.isna(row.get('법령명_한자')) else None,
                        str(row.get('법령명약칭', '')) if not pd.isna(row.get('법령명약칭')) else None,
                        str(row.get('공포번호', '')) if not pd.isna(row.get('공포번호')) else None,
                        str(row.get('공포일자', '')) if not pd.isna(row.get('공포일자')) else None,
                        promulgation_date,
                        str(row.get('시행일자', '')) if not pd.isna(row.get('시행일자')) else None,
                        enforcement_date,
                        int(row['유예기간_일수']) if not pd.isna(row.get('유예기간_일수')) else None,
                        str(row.get('제개정구분', '')) if not pd.isna(row.get('제개정구분')) else None,
                        str(row.get('언어', '')) if not pd.isna(row.get('언어')) else None,
                        str(row.get('전화번호', '')) if not pd.isna(row.get('전화번호')) else None,
                        str(row.get('편장절관', '')) if not pd.isna(row.get('편장절관')) else None,
                        str(row.get('공포법령여부', '')) if not pd.isna(row.get('공포법령여부')) else None,
                        str(row.get('개정문내용', '')) if not pd.isna(row.get('개정문내용')) else None,
                        str(row.get('제개정이유내용', '')) if not pd.isna(row.get('제개정이유내용')) else None,
                        int(row['총_조문수']) if not pd.isna(row.get('총_조문수')) else None,
                        int(row['총_부칙수']) if not pd.isna(row.get('총_부칙수')) else None,
                        processing_month,
                        datetime.now(),
                        datetime.now()
                    )

                    records.append(record)
                    self.success_count += 1

                except Exception as e:
                    logger.error(f"레코드 준비 실패 (행 {idx}): {e}")
                    self.error_count += 1

        finally:
            conn.close()

        return records

    def bulk_insert_law_data_with_ids(self, records):
        """대량 법령 데이터 삽입 후 ID 매핑 반환"""
        if not records:
            return {}

        insert_sql = """
           INSERT INTO law_data (
               law_key, law_id, law_name_korean, law_name_chinese, law_name_abbreviation,
               promulgation_number, promulgation_date_str, promulgation_date,
               enforcement_date_str, enforcement_date, grace_period_days, revision_type,
               language, phone_number, chapter_section, is_promulgated_law,
               revision_content, revision_reason,
               total_articles, total_addenda, processing_month, created_at, updated_at
           ) VALUES %s
           ON CONFLICT (law_key, processing_month) 
           DO UPDATE SET
               law_name_korean = EXCLUDED.law_name_korean,
               updated_at = EXCLUDED.updated_at
           RETURNING id, law_key
           """

        conn = None
        cursor = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()

            execute_values(
                cursor,
                insert_sql,
                records,
                template=None,
                page_size=500
            )

            # ID와 law_key 매핑 생성
            id_mappings = {}
            for row in cursor.fetchall():
                law_data_id, law_key = row
                id_mappings[law_key] = law_data_id

            conn.commit()
            logger.info(f"대량 삽입 완료: {len(id_mappings)}건")
            return id_mappings

        except Exception as e:
            logger.error(f"대량 삽입 실패: {e}")
            if conn:
                conn.rollback()
                logger.info("트랜잭션 롤백 완료")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def save_top_laws_by_category(self, category_results, processing_month):
        """각 카테고리별 상위 5개 법령을 DB에 저장"""
        if not category_results:
            return 0

        conn = None
        cursor = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            total_saved = 0

            for category_code, laws in category_results.items():
                # 점수 순으로 정렬 후 상위 5개 선별
                sorted_laws = sorted(laws, key=lambda x: x['priority_score'], reverse=True)
                top_5_laws = sorted_laws[:5]

                # 카테고리 ID 조회
                cursor.execute("SELECT id FROM law_category WHERE code = %s", (category_code,))
                category_result = cursor.fetchone()
                if not category_result:
                    logger.warning(f"카테고리 {category_code} 를 찾을 수 없음")
                    continue

                category_id = category_result['id']
                logger.info(f"카테고리 {category_code}: {len(laws)}개 중 상위 {len(top_5_laws)}개 저장")

                # 상위 법령들 저장
                for rank, law_info in enumerate(top_5_laws, 1):
                    try:
                        # law_data_id 조회
                        cursor.execute(
                            "SELECT id FROM law_data WHERE law_key = %s AND processing_month = %s",
                            (str(law_info['law_key']), processing_month)
                        )
                        law_result = cursor.fetchone()
                        if not law_result:
                            logger.warning(f"법령키 {law_info['law_key']} 를 찾을 수 없음")
                            continue

                        law_data_id = law_result['id']

                        # processed_law 테이블에 삽입 (created_at도 추가)
                        cursor.execute("""
                               INSERT INTO processed_law (
                                   law_data_id, category_id, processing_month, 
                                   priority_score, category_rank, score_breakdown, 
                                   category_scores, impact_keywords, created_at
                               ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                           """, (
                            law_data_id,
                            category_id,
                            processing_month,
                            law_info['priority_score'],
                            rank,
                            json.dumps(law_info['score_breakdown'], ensure_ascii=False),
                            json.dumps(law_info['category_scores'], ensure_ascii=False),
                            json.dumps(law_info['impact_keywords'], ensure_ascii=False)
                        ))

                        total_saved += 1

                    except Exception as e:
                        logger.error(f"개별 법령 저장 실패 (카테고리: {category_code}, 순위: {rank}): {e}")
                        continue

            conn.commit()
            logger.info(f"카테고리별 상위 법령 저장 완료: 총 {total_saved}건")
            return total_saved

        except Exception as e:
            logger.error(f"카테고리별 법령 저장 실패: {e}")
            if conn:
                conn.rollback()
                logger.info("트랜잭션 롤백 완료")
            return 0
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def delete_existing_data(self, processing_month):
        """기존 데이터 삭제"""
        conn = None
        cursor = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()

            # law_data 삭제 (CASCADE로 processed_law도 함께 삭제됨)
            cursor.execute(
                "DELETE FROM law_data WHERE processing_month = %s",
                (processing_month,)
            )
            deleted_count = cursor.rowcount
            conn.commit()

            logger.info(f"{processing_month} 기존 데이터 {deleted_count}건 삭제")
            return deleted_count

        except Exception as e:
            logger.error(f"기존 데이터 삭제 실패: {e}")
            if conn:
                conn.rollback()
                logger.info("트랜잭션 롤백 완료")
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def load_csv_to_rds(self, processing_month, csv_s3_key=None, replace_existing=True):
        """CSV 파일을 RDS에 직접 로드 (카테고리별 점수화 포함)"""
        start_time = datetime.now()

        try:
            logger.info(f"{processing_month} RDS 직접 로드 시작 (카테고리별 점수화 포함)")

            # 테이블 생성/확인
            self.create_django_compatible_tables()

            # CSV 파일 경로 결정
            if not csv_s3_key:
                csv_s3_key = f'processed_data/{processing_month}/{processing_month}_processed_eflaw_data.csv'

            # CSV 파일 로드
            df = self.load_csv_from_s3(csv_s3_key)
            if df is None or df.empty:
                return {
                    'statusCode': 404,
                    'body': {'error': f'{csv_s3_key} 파일을 찾을 수 없거나 비어있습니다.'}
                }

            # 기존 데이터 삭제
            deleted_count = 0
            if replace_existing:
                deleted_count = self.delete_existing_data(processing_month)

            # 법령 데이터 + 카테고리별 점수화 통합 처리
            processing_result = self.process_laws_with_categories(df, processing_month)

            result = {
                'statusCode': 200,
                'body': {
                    'message': f'{processing_month} RDS 직접 로드 완료 (카테고리별 점수화 포함)',
                    'csv_file': csv_s3_key,
                    'deleted_count': deleted_count,
                    'processing_result': processing_result,
                    'success_count': self.success_count,
                    'error_count': self.error_count,
                    'bucket': self.bucket_name
                }
            }

            logger.info(f"{processing_month} RDS 직접 로드 완료")
            logger.info(f"- 삭제: {deleted_count}건")
            logger.info(f"- 처리: {processing_result['total_processed']}건")
            logger.info(f"- 카테고리: {processing_result['categories_found']}개")
            logger.info(f"- 상위 법령 저장: {processing_result['top_laws_saved']}건")

            return result

        except Exception as e:
            end_time = datetime.now()
            duration = int((end_time - start_time).total_seconds())

            logger.error(f"RDS 직접 로드 실패: {e}")
            return {
                'statusCode': 500,
                'body': {'error': f'데이터베이스 로드 실패: {str(e)}'}
            }


def lambda_handler(event, context):
    """Lambda 핸들러 함수"""
    try:
        logger.info(f"RDS 직접 법령 데이터 로드 시작 (카테고리 포함) - event: {event}")

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

        loader = RDSLawDataLoader()

        # 파라미터 추출
        year = event.get('year') if isinstance(event, dict) else None
        month = event.get('month') if isinstance(event, dict) else None
        month_str = event.get('month_str') if isinstance(event, dict) else None
        csv_s3_key = event.get('csv_s3_key') if isinstance(event, dict) else None
        replace_existing = event.get('replace_existing', True) if isinstance(event, dict) else True

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

        # RDS 직접 로드 실행 (카테고리별 점수화 포함)
        result = loader.load_csv_to_rds(month_str, csv_s3_key, replace_existing)

        logger.info(f"RDS 직접 법령 데이터 로드 완료")
        return result

    except Exception as e:
        logger.error(f'Lambda 실행 오류: {e}')
        return {
            'statusCode': 500,
            'body': {'error': str(e)}
        }
