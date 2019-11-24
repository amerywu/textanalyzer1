#!/usr/bin/env bash


mv ./Dockerfile ./Dockerfile.d
#scp -r /Users/jake.stone/_vws/com.merm.enablement.scripts/textanalyzer jake.stone@usw2-srfb01.office.merminc.com:/home/VI/jake.stone/sirf/textanalyzer/deployment
scp -r /Users/jake.stone/_vws/com.merm.enablement.scripts/textanalyzer jake.stone@usw2-srff01:/home/VI/jake.stone/textanalyzer/deployment

mv ./Dockerfile.d ./Dockerfile
