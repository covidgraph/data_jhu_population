FROM python:3

RUN mkdir /src
RUN mkdir /download

# copy covid_graph package
COPY covid_graph /src/covid_graph
# copy run.py script
COPY run.py /src/
COPY requirements.txt /src/
# copy tests
COPY tests /src/test

WORKDIR /src

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH="/src:${PYTHONPATH}"

CMD ["python", "run.py"]