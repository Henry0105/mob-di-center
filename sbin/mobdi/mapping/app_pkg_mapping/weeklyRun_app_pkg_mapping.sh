#! /bin/sh
: '
@owner: qingy
@describe: 初步包名渠道清理
@projectName:MobDI
@BusinessName:mapping
@SourceTable:dm_sdk_mapping.app_pkg_mapping_par,dw_mobdi_md.app_pkg_mapping,dm_sdk_mapping.app_pkg_clean_byhand,dw_mobdi_md.master_pkg_name,dw_mobdi_md.master_pkg_size${n}_final,dm_sdk_mapping.pkg_from_channel,dw_mobdi_md.master_pkg_size${n}_$s dw_mobdi_md.master_pkg dm_sdk_mapping.pkg_name_mapping

@TargetTable:dw_mobdi_md.master_pkg_name,dw_mobdi_md.master_pkg,table dw_mobdi_md.app_pkg_mapping,dw_mobdi_md.master_pkg_size3_2,dw_mobdi_md.master_pkg_size4_3,dw_mobdi_md.master_pkg_size5_4,dw_mobdi_md.master_pkg_size${n}_final,dm_sdk_mapping.app_pkg_mapping_par

@TableRelation:dm_sdk_mapping.pkg_name_mapping,dm_sdk_mapping.pkg_from_channel->dm_sdk_mapping.app_pkg_mapping_par
'
t1=$1;
set -x +e 

if [ $(date -d "$t1" +%w) -eq 2 ] ;then
: '
@part_1:
实现功能：获得主渠道
实现逻辑：通过包名各个部分和pkg_from_channel匹配获得主渠道
输出结果：app_pkg_mapping
'

app_pkg_clean_byhand_sql="
    add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    create temporary function GET_LAST_PARTITION as 'com.youzu.mob.java.udf.LatestPartition';
    SELECT GET_LAST_PARTITION('dim_sdk_mapping', 'dim_app_pkg_clean_byhand', 'version');
"
app_pkg_clean_byhand_partition=(`hive -e "$app_pkg_clean_byhand_sql"`)

#hive -e "
#create table if not exists dm_mobdi_tmp.master_pkg_name(pkg string, name string, arr array<string>, arr_size int);
#create table if not exists dm_mobdi_tmp.master_pkg(pkg string, arr array<string>, arr_size int);
#create table if not exists dm_mobdi_tmp.app_pkg_mapping(pkg string, apppkg string, tag tinyint);
#create table if not exists dm_mobdi_tmp.master_pkg_size3_2(pkg string, arr array<string>, arr_size int, p_0 string, p_1 string, p_2 string);
#create table if not exists dm_mobdi_tmp.master_pkg_size3_final(pkg string, p_0 string, p_1 string, p_2 string);
#create table if not exists dm_mobdi_tmp.master_pkg_size4_3(pkg string, arr array<string>, arr_size int, p_0 string, p_1 string, p_2 string, p_3 string);
#create table if not exists dm_mobdi_tmp.master_pkg_size4_final(pkg string, p_0 string, p_1 string, p_2 string, p_3 string);
#create table if not exists dm_mobdi_tmp.master_pkg_size5_4(pkg string, arr array<string>, arr_size int, p_0 string, p_1 string, p_2 string, p_3 string, p_4 string);
#create table if not exists dm_mobdi_tmp.master_pkg_size5_final(pkg string, p_0 string, p_1 string, p_2 string, p_3 string, p_4 string);
#";
	   sql="insert overwrite table dm_mobdi_tmp.master_pkg_name
	        select t.*,size(arr) arr_size
	        from
	        (
			  select pkg,trim(split(name,\"[-—@{}<>=:：《()（）]\")[0]) name,split(pkg,\"\\\.\") arr
			  from
			  (
			    select pkg,name
			    from
			    (
			      select pkg,name
			      from dim_mobdi_mapping.dim_app_name_info_orig
			    )a
			    group by pkg,name
			  )c
			)t
			where length(pkg) > 3
			and pkg = regexp_extract(pkg,'([a-zA-Z0-9\.\_]+)',0)";
	   echo "$sql"; hive -e "$sql";

	   sql="insert overwrite table dm_mobdi_tmp.master_pkg
	        select pkg,arr,arr_size
			from dm_mobdi_tmp.master_pkg_name
			group by pkg,arr,arr_size";
	   echo "$sql"; hive -e "$sql";
//啥呀
	   sql="insert overwrite table dm_mobdi_tmp.app_pkg_mapping
	        select pkg,apppkg,tag
			from dim_sdk_mapping.dim_app_pkg_mapping_par
			where version='1000'";
	   echo "$sql"; hive -e "$sql";

	   arr_size=(3 4 5);

	   sql="insert overwrite table dm_mobdi_tmp.master_pkg_size3_2
	        select pkg,arr,arr_size,arr[0] as p_0,arr[1] as p_1,arr[2] as p_2
	        from dm_mobdi_tmp.master_pkg
	        where arr_size = 3";
	   echo "$sql"; hive -e "$sql";

	   sql="insert overwrite table dm_mobdi_tmp.master_pkg_size4_3
	        select pkg,arr,arr_size,arr[0] as p_0,arr[1] as p_1,arr[2] as p_2,arr[3] as p_3
	        from dm_mobdi_tmp.master_pkg
	        where arr_size = 4";
	   echo "$sql"; hive -e "$sql";

	   sql="insert overwrite table dm_mobdi_tmp.master_pkg_size5_4
	        select pkg,arr,arr_size,arr[0] as p_0,arr[1] as p_1,arr[2] as p_2,arr[3] as p_3,arr[4] as p_4
	        from dm_mobdi_tmp.master_pkg
	        where arr_size = 5";
	   echo "$sql"; hive -e "$sql";

	   if [ ${#arr_size[*]} -ge 1 ];then

		  for n in ${arr_size[*]};do

			 col=""; a_col=""; subsql=""; s=`expr $n - 1`; u=`expr $n - 2`;

			 for ((k=1; k<$n; ++k));do
				m=`expr $k - 1`;
				col="$col p_$m,"; a_col="${a_col} a.p_$m,";
				subsql="${subsql} a.p_$m = b.p_$m and";
			 done

			 for ((w=2; w<$n; ++w));do
				head=${col%p_$w*};
				if [ $w -ne $s ];then
				   tail_col=",${col#*p_$w,} p_$s";
				   if [ $w = 2 ];then
					  table="dm_mobdi_tmp.master_pkg_size${n}_$s";
					  sql1="insert overwrite table dm_mobdi_tmp.master_pkg_size${n}_final";
				   else
					  table="dm_mobdi_tmp.master_pkg_size${n}_final";
					  sql1="insert overwrite table dm_mobdi_tmp.master_pkg_size${n}_final";
				   fi
				else
				   tail_col="";
				   if [ $n = 3 ];then
					  table="dm_mobdi_tmp.master_pkg_size${n}_$s";
					  sql1="insert overwrite table dm_mobdi_tmp.master_pkg_size${n}_final";
				   else
					  table="dm_mobdi_tmp.master_pkg_size${n}_final";
					  sql1="insert overwrite table dm_mobdi_tmp.master_pkg_size${n}_final";
				   fi
				fi

				sql="$sql1 select pkg,${head}COALESCE(t.main_channel,p_$w) p_$w${tail_col} from
					 $table tt left join dim_sdk_mapping.dim_pkg_from_channel t on tt.p_$w = t.channel";
				echo "$sql"; hive -e "$sql";
			 done

			 if [ $n -le 4 ];then
				subsql="p_$s = \"yz_zsy\"";
			 else
				subsql="((p_4 = \"yz_zsy\" and p_3 <> \"yz_zsy\" and p_2 <> \"yz_zsy\")
						or (p_4 = \"yz_zsy\" and p_3 = \"yz_zsy\" and p_2 <> \"yz_zsy\")
						or (p_4 = \"yz_zsy\" and p_3 = \"yz_zsy\" and p_2 = \"yz_zsy\")) ";
			 fi

			 sql="insert into table dm_mobdi_tmp.app_pkg_mapping
			      select pkg,apppkg,1 tag
			      from
			      (
				    select pkg,split("apppkg",\".yz_zsy\")[0] apppkg
				    from
				    (
				      select pkg,apppkg
				      from
				      (
				        select pkg,concat_ws(\".\",${col}p_$s) apppkg
				        from dm_mobdi_tmp.master_pkg_size${n}_final
				        where $subsql
				      )t
				      group by pkg,apppkg
				    )tt
				  )ttt
				  where (apppkg <> \"cn.com\"
				  and apppkg <> \"com.android\" and apppkg <> \"com.tencent\" and apppkg <> \"com.baidu\"
				  and apppkg <> \"com.tencent.tmgp\" and apppkg <> \"com.uuzu.nslm\" and apppkg <> \"com.youzu.dahuangdi\")";
			 echo "$sql"; hive -e "$sql";

		  done

: '
@part_2:
实现功能：获得初步渠道清理数据
输出结果：app_pkg_mapping
'
		sql="insert into table dm_mobdi_tmp.app_pkg_mapping
		     select pkg,apppkg,0 tag
		     from
		     (
			   select *,
			          row_number() over (partition by pkg order by diff asc) rank
			   from
			   (
			     select pkg,apppkg,(length(pkg)-length(apppkg)) diff
			     from
			     (
			       select tt.apppkg pkg,t.apppkg
			       from
			       (
			         select apppkg,app_name
			         from
			         (
			           select *,
			                  count(1) over (partition by app_name) cnt
			           from
			           (
			             select apppkg,app_name
			             from
			             (
			               select apppkg,app_name,
			                      row_number() over (partition by apppkg order by cnt desc) rank
			               from
			               (
			                 select apppkg,app_name,count(*) cnt
			                 from
			                 (
			                   select a.apppkg,b.name app_name f
			                   rom dm_mobdi_tmp.app_pkg_mapping a
			                   left join
			                   dm_mobdi_tmp.master_pkg_name b on a.pkg = b.pkg
			                   where length(a.apppkg) >= 7
			                 )c
			                 where app_name is not null
			                 group by apppkg,app_name
			               )d
			             )e
			             where rank = 1
			           )tt
			         )ts
			         where cnt < 20
			       )t
			       inner join
			       (
			         select pkg apppkg,name app_name
			         from dm_mobdi_tmp.master_pkg_name
			       ) tt on (t.app_name = tt.app_name)
			       where regexp_extract(tt.apppkg,t.apppkg,0)=t.apppkg
			     )ttt
			     group by pkg,apppkg
			   )tttt
			 )ttttt
			 where rank = 1";
		echo "$sql"; hive -e "$sql";

		sql="insert into table dm_mobdi_tmp.app_pkg_mapping
		     select pkg,apppkg,2 as tag
		     from dim_sdk_mapping.dim_app_pkg_clean_byhand where version = '$app_pkg_clean_byhand_partition' ;";
		echo "$sql"; hive -e "$sql";
        #添加应用宝清洗过程
        sql="
        insert overwrite table dm_mobdi_tmp.pkg_yyb_to_wdj_cln
        select a.packagename_yyb as pkg,b.apppkg as apppkg,b.tag as tag
        from
        (
          select packagename_yyb,packagename_wdj
          from dim_sdk_mapping.dim_yyb_pkg_cln
        ) a
        inner join
        (
          select *
          from dm_mobdi_tmp.app_pkg_mapping
        ) b
        on a.packagename_wdj=b.pkg；
        ";
        echo "$sql"; hive -e "$sql";
        sql="
        insert overwrite table dm_mobdi_tmp.app_pkg_mapping
        select * from  dm_mobdi_tmp.app_pkg_mapping
        union all
        select * from dm_mobdi_tmp.pkg_yyb_to_wdj_cln
        ;
        ";
        echo "$sql"; hive -e "$sql";


		 sql="insert overwrite table dim_sdk_mapping.dim_app_pkg_mapping_par partition (version='${t1}.1000')
		      select pkg,apppkg,tag
		      from
		      (
		        select *,
			           row_number() over (partition by pkg order by tag desc) rank
			    from dm_mobdi_tmp.app_pkg_mapping
			  )a
			  where rank = 1";
		 echo "$sql"; hive -e "$sql";

        hive -v -e "
        insert overwrite table dim_sdk_mapping.dim_app_pkg_mapping_par partition (version='1000')
        select pkg,apppkg,tag
        from dim_sdk_mapping.dim_app_pkg_mapping_par
        where version='${t1}.1000';"
       fi
	

	t2=$(date);

	td=$(($(date +%s -d "$t2") - $(date +%s -d "$t1")));
	min=`echo "scale=2; $td/60"|bc`;
	echo "This script starts at $t1, ends at $t2, and it lasts $min minutes!";
fi
