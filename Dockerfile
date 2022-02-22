FROM python:3.10.0
ADD config.py .
ADD app.py .
COPY requirements.txt .
RUN pip install -r requirements.txt
#CMD [ "python","./app.py","-k","-d","-p"]
ENTRYPOINT [ "python","./app.py" ]