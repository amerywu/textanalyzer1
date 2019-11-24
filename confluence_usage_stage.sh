#!/usr/bin/env bash


mv ./Dockerfile ./Dockerfile.d
#scp -r /Users/jake.stone/_vws/com.merm.enablement.scripts/textanalyzer jake.stone@usw2-srfb01.office.merminc.com:/home/VI/jake.stone/sirf/textanalyzer/deployment
scp -r /Users/jake.stone/_vws/com.merm.enablement.scripts/textanalyzer jake.stone@usw2-sirf01a:/home/VI/jake.stone/confluenceUsage

mv ./Dockerfile.d ./Dockerfile
