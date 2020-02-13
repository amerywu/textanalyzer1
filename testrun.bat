cd ..
@RD /S /Q .\testrun
XCOPY .\textanalyzer1 .\testrun /s /i
cd testrun
python TextAnalyzerLaunch.py
cd ..\textanalyzer1