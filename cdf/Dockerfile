FROM python:3.9.12-buster

WORKDIR /code

# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#        gdal-bin \
#        libgdal-dev \
#     && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["jupyter", "notebook", "--port=8888", "--no-browser", "--ip=0.0.0.0", "--allow-root"]