FROM python:3-onbuild
ENV NLTK_DATA=./resources/nltk_data

RUN python -m  nltk.downloader -d$NLTK_DATA wordnet

CMD  ["python3", "TextAnalyzerLaunch.py"]
