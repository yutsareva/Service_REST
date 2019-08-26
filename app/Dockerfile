FROM python:3.6
EXPOSE 8080
RUN mkdir /app
COPY requirements.txt /app
WORKDIR /app
RUN pip install -r requirements.txt
COPY app.py /app
COPY models.py /app
CMD python app.py
