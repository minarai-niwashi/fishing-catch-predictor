FROM public.ecr.aws/lambda/python:3.12

RUN dnf install -y libgomp && dnf clean all

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
