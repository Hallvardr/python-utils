#!/bin/bash
## declare an array variable
rds_user=dv_saas_monitor
rds_pass=dv_saas_monitor
rds_endpoint=rds-ibs-d-ew1-m-kyceud-mariadb-4201.c1jqedxissay.eu-west-1.rds.amazonaws.com
local_path=/opt/g2m/cron_task/demo_$(aws-current-region --short)/input/
i=dv_saas_demo

s3_path=""${s3_path}""
S3Uri_sql="$s3_path""/cron_task/sql/"

aws s3 sync /opt/g2m/cron_task/demo_$(aws-current-region --short)/sql $S3Uri_sql

declare -a block=("report")
for m in "${block[@]}"
do

# starting block part
LocalPath="$local_path$m"
S3Uri_input="$s3_path""/cron_task/sql/""$m"
S3Uri_output="$s3_path""/cron_task/weekly_extractions/""$m"
echo "$S3Uri_input"
aws s3 sync $S3Uri_input $LocalPath --exclude '*' --include "weekly_report.sql"

# starting block concatenation
current_time=$(date "+%d_%m_%Y")
        echo date
        for sql_file in `ls "$LocalPath"/*.sql`;
        do
        file=`echo "$sql_file" | cut -d'.' -f1`
        customer="$LocalPath"/"Demo_report_"
        nohup mysql -h $rds_endpoint -u $rds_user -p$rds_pass -D $i -e "set @schemas='${i}'; source $sql_file;" > $sql_file.$i.$current_time.csv;
        tr '\t' ';' < $sql_file.$i.$current_time.csv > $customer$current_time.csv
        aws s3 sync $LocalPath $S3Uri_output --exclude '*' --include $customer$current_time.csv
        done
####
rm $LocalPath/*.sql -f
#rm $LocalPath/*.csv -f
done