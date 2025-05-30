# 이거 리눅스 환경에서 실행되는거라 해야됨
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.10

RUN yum update -y && \
    yum install -y postgresql-devel gcc python3-devel

COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install --no-cache-dir -r requirements.txt

COPY lambda_function.py ${LAMBDA_TASK_ROOT}

CMD ["lambda_function.lambda_handler"]
