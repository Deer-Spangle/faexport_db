FROM python:3.8

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:$PATH"

COPY . .
RUN poetry install
EXPOSE 5000

ENV FLASK_APP="main.py"
ENTRYPOINT ["flask", "run", "--host", "0.0.0.0"]
