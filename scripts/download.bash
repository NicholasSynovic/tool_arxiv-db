#!/bin/bash
source optparse.bash

optparse.define short=a long=kaggle-api-key desc="Kaggle API key" variable=KAGGLE_KEY
optparse.define short=o long=output desc="Directory to save the file to" variable=outputDir default=../data
optparse.define short=u long=kaggle-username desc="Kaggle username" variable=KAGGLE_USERNAME

source $( optparse.build )

if [[ -z $KAGGLE_USERNAME ]]; then
    echo "No Kaggle username provided."
    exit 1
fi

if [[ -z $KAGGLE_KEY ]]; then
    echo "No Kaggle API key provided."
    exit 1
fi

absOutputDir=$(realpath $outputDir)

kaggle datasets download -p $absOutputDir --unzip -d Cornell-University/arxiv
