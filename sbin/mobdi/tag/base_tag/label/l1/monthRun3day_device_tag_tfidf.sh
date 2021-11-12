#! /bin/sh
set -x -e

export LANG=en_US.UTF-8
: '
@owner: zhtli
@describe: dailyrun-tfidf全量数据的计算(每月3号跑，依赖于一号的dailyrun已经跑完)-->dailyrun升级为mobdi之后，依赖mobdi跑完
@projectName:MobDI
@BusinessName:tag_full_calcu
@SourceTable:dw_mobdi_md.device_tag_tf,dm_mobdi_mapping.device_applist_new,dw_mobdi_md.top_1000_device_applist,dm_sdk_mapping.tag_id_mapping_par,dm_sdk_mapping.app_tag_system_mapping_par,dw_mobdi_md.tag_idf
@TargetTable:dw_mobdi_md.top_1000_device_applist,dw_mobdi_md.device_tag_tf,dw_mobdi_md.tag_idf,rp_mobdi_app.rp_device_label_profile
@TableRelation:dm_mobdi_mapping.device_applist_new,dw_mobdi_md.device_tag_tf,dw_mobdi_md.tag_idf->rp_mobdi_app.rp_device_label_profile
'

#baseLibPath="/home/dba/taskScheduleSystem/lib"
#send_mail_path="$baseLibPath/APPGO.ETL-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
DATE=$(date  +%Y%m%d)
: '
@parameters
@last_par:正常跑不传参数，手动跑传入applist的一个分区
'

source /home/dba/mobdi_center/conf/hive-env.sh

tmp=${dm_mobdi_tmp}

#input
device_applist_new=${dim_device_applist_new_di}

#tmp
top_1000_device_applist=${tmp}.top_1000_device_applist

#output
device_tag_tf=${tmp}.device_tag_tf
tag_idf=${tmp}.tag_idf

sql="show partitions $device_applist_new"; par=(`hive -e "$sql"`);
last_par="${par[(${#par[*]}-1)]#*=}";
echo "last partition in table $device_applist_new: ${last_par}";

: '
@part_2:
实现功能：从dm_mobdi_mapping.device_applist_new中取出安装量top1000的app

实现逻辑：从dm_mobdi_mapping.device_applist_new中取出安装量top1000的app
输出结果：dm_mobdi_mapping.device_applist_new中安装量top1000的app
			pkg    app名字
			cnt    安装量
'
sql="
set hive.exec.reducers.bytes.per.reducer=1070596096;
insert overwrite table $top_1000_device_applist
select pkg,count(1) cnt from $device_applist_new where day=${last_par}
group by pkg
order by  cnt desc
limit 1000";
hive -e "$sql";

: '
@part_3:
实现功能：从dm_mobdi_mapping.device_applist_new计算出全量的tfidf

实现逻辑：1.dm_sdk_mapping.app_tag_system_mapping_par join dm_sdk_mapping.tag_id_mapping_par 获取到tag id，并取出norm最大的apppkg,tag
		  2.applist join 第一步的结果 group by device,tag sum 出tf
		  3.算出tf在单个app对用户所有app的比例
输出结果：dw_mobdi_md.device_tag_tf 
			device    设备号
			tag    标签
			tf     tf
'

sql="
set hive.exec.reducers.bytes.per.reducer=1070596096;
set hive.exec.parallel=true; 
insert overwrite table $device_tag_tf SELECT
	device,
	tag,
	tfidf / sum(tfidf) over (PARTITION BY device) tf
FROM
	(
		SELECT
			device,
			tag,
			sum(norm_tfidf) tfidf
		FROM
			(
				SELECT
					a.device,
					COALESCE (b.tag, \"其它\") tag,
					COALESCE (b.norm_tfidf, 0) norm_tfidf
				FROM
					$device_applist_new a
				JOIN (
					SELECT
						aa.*
					FROM
						(
							SELECT
								apppkg,
								tag,
								norm_tfidf
							FROM
								(
									SELECT
										apppkg,
										cate,
										tag,
										norm_tfidf,
										row_number () over (
											PARTITION BY apppkg,
											tag
										ORDER BY
											norm_tfidf DESC
										) rank
									FROM
										(
											SELECT
												apppkg,
												cate,
												b.id tag,
												norm_tfidf
											FROM
												(select * from $dim_app_tag_system_mapping_par where version='1000') a
											JOIN (select * from $dim_tag_id_mapping_par where version='1000') b ON a.tag = b.tag
										) t
									WHERE
										norm_tfidf > 0
								) a
							WHERE
								rank = 1
						) aa
					WHERE
						aa.apppkg IN (
							SELECT
								pkg
							FROM
								$top_1000_device_applist
						)
				) b ON a.pkg = b.apppkg
				WHERE
					a. DAY = ${last_par}
				UNION ALL
					SELECT
						a.device,
						COALESCE (b.tag, \"其它\") tag,
						COALESCE (b.norm_tfidf, 0) norm_tfidf
					FROM
						$device_applist_new a
					JOIN (
						SELECT
							aa.*
						FROM
							(
								SELECT
									apppkg,
									tag,
									norm_tfidf
								FROM
									(
										SELECT
											apppkg,
											cate,
											tag,
											norm_tfidf,
											row_number () over (
												PARTITION BY apppkg,
												tag
											ORDER BY
												norm_tfidf DESC
											) rank
										FROM
											(
												SELECT
													apppkg,
													cate,
													b.id tag,
													norm_tfidf
												FROM
													(select * from $dim_app_tag_system_mapping_par where version='1000') a
												JOIN (select * from $dim_tag_id_mapping_par where version='1000') b ON a.tag = b.tag
											) t
										WHERE
											norm_tfidf > 0
									) a
								WHERE
									rank = 1
							) aa
						WHERE
							aa.apppkg NOT IN (
								SELECT
									pkg
								FROM
									$top_1000_device_applist
							)
					) b ON a.pkg = b.apppkg
					WHERE
						a. DAY = ${last_par}
			) c
		GROUP BY
			device,
			tag
	) d";
hive -e "$sql";


sql="
insert overwrite table $tag_idf
select a.tag,a.tag_freq,b.count_dev,ln((b.count_dev+1)/(a.tag_freq+1)) idf
from
(
  select tag,count(1) tag_freq
  from $device_tag_tf
  group by tag
) a
left join
(
  select count(1) count_dev
  from
  (
    select device
    from $device_tag_tf
    group by device
  )t
) b";
echo "$sql"; hive -e "$sql";