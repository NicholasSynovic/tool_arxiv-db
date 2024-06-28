#!/bin/bash
source optparse.bash

optparse.define short=f long=file desc="The file to process" variable=file
optparse.define short=o long=output desc="The output file" variable=output default=head_output.txt
optparse.define short=l long=lines desc="The number of lines to head (default:5)" variable=lines default=5
optparse.define short=v long=verbose desc="Flag to set verbose mode on" variable=verbose_mode value=true default=false

source $( optparse.build )
