FROM rayproject/ray:2.38.0-py310

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ray_pipeline.py /app/ray_pipeline.py

ENV PYTHONUNBUFFERED=1
CMD ["python", "/app/ray_pipeline.py"]
