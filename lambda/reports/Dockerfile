FROM public.ecr.aws/lambda/python:3.11

# Install python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy function code
COPY index.py ${LAMBDA_TASK_ROOT}/index.py

CMD ["index.handler"]
