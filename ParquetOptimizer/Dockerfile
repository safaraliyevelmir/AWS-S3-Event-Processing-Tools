FROM public.ecr.aws/lambda/python:3.10

ENV \
  # python:
  PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PYTHONDONTWRITEBYTECODE=1 \
  # pip:
  PIP_NO_CACHE_DIR=1 \
  PIP_DISABLE_PIP_VERSION_CHECK=1 \
  PIP_DEFAULT_TIMEOUT=100

WORKDIR ${LAMBDA_TASK_ROOT}
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY main.py main.py

CMD [ "main.lambda_handler" ]
