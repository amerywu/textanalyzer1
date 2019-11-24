#!/bin/bash


mv ./Dockerfile ./Dockerfile.d
scp -r /Users/jake.stone/_vws/com.merm.enablement.scripts/textanalyzer jake.stone@L-0BVJXHN2:/home/VI/jake.stone/textanalyzer/deployment
mv ./Dockerfile.d ./Dockerfile

